{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

module OCD (backfill) where

import Control.Monad.IO.Class
import Control.Monad.Logger (runStdoutLoggingT)
import qualified Data.ByteString.Lazy as BL
import Data.Csv
import Data.Functor ((<&>))
import Data.Int
import Data.Maybe
import Data.Text (Text)
import Data.Time (UTCTime, defaultTimeLocale, parseTimeM)
import qualified Data.Vector as V
import Database.Persist.Postgresql
import Database.Persist.TH

data OcInfo = OcInfo {outcropId :: Int64, insertedAt :: UTCTime} deriving (Show)

instance FromNamedRecord OcInfo where
  parseNamedRecord m =
    OcInfo
      <$> m .: "ID"
      <*> (parseDate =<< m .: "Inserted At")
    where
      parseDate :: String -> Parser UTCTime
      parseDate = parseTimeM True defaultTimeLocale "%Y-%m-%d %T"

parseCsvFile :: String -> IO (Either String [OcInfo])
parseCsvFile filePath = do
  csvData <- BL.readFile filePath
  case decodeByNameWith decodeOpts csvData :: Either String (Header, V.Vector OcInfo) of
    Left err -> pure $ Left err
    Right (_, v) -> pure $ Right (V.toList v)
  where
    decodeOpts = defaultDecodeOptions {decDelimiter = fromIntegral (fromEnum ';')}

share
  [mkPersist sqlSettings]
  [persistLowerCase|
AuditLog sql=audit_logs
    entity Text
    action Text
    userId Int64
    outcropId Int64 Maybe
    studyId Int64 Maybe
    insertedAt UTCTime
    deriving Show
|]

execQuery :: SqlPersistT IO a -> IO a
execQuery action = runStdoutLoggingT $
  withPostgresqlConn connString $
    \backend -> liftIO $ runSqlConn action backend
  where
    connString = "postgresql://postgres:postgres@localhost:5432/safari_api"

ocIdsWithExistingLogs :: [Int64] -> IO [Int64]
ocIdsWithExistingLogs outcropIds = execQuery $ do
  selectList
    [AuditLogEntity ==. "outcrop", AuditLogOutcropId <-. map Just outcropIds]
    []
    <&> mapMaybe (auditLogOutcropId . entityVal)

insertNewLogs :: [(Int64, UTCTime)] -> IO (Int, Int)
insertNewLogs toBeCreated = execQuery (doInsert toBeCreated (0, 0))
  where
    doInsert :: [(Int64, UTCTime)] -> (Int, Int) -> SqlPersistT IO (Int, Int)
    doInsert [] (success, failed) = pure (success, failed)
    doInsert ((ocId, inserted) : xs) (success, failed) = do
      insertResult <- insert $ AuditLog "outcrop" "created" 11 (Just ocId) Nothing inserted
      if insertResult == toSqlKey 0
        then doInsert xs (success + 1, failed)
        else doInsert xs (success, failed + 1)

backfill :: IO ()
backfill = do
  parseResult <- parseCsvFile "dates.csv"
  case parseResult of
    Left err -> putStrLn $ "Error parsing CSV:" ++ err
    Right ocInfos -> do
      ocIds <- ocIdsWithExistingLogs $ map outcropId ocInfos
      putStrLn $ "Found " ++ show (length ocIds) ++ " existing OCs with logs"
      let toInsert = filter (\ocInfo -> outcropId ocInfo `notElem` ocIds) ocInfos
      putStrLn $ show (length toInsert) ++ " records to be created"
      (success, failed) <- insertNewLogs $ map (\ocInfo -> (outcropId ocInfo, insertedAt ocInfo)) toInsert
      putStrLn $ "Created " ++ show success ++ " successful logs with " ++ show failed ++ " failed."