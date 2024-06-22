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

module OCD.Database (ocIdsWithExistingLogs, insertNewLogs) where

import Control.Monad.IO.Class
import Control.Monad.Logger (runStdoutLoggingT)
import Data.Functor ((<&>))
import Data.Int
import Data.Maybe
import Data.Text (Text)
import Data.Time (UTCTime)
import Database.Persist.Postgresql
import Database.Persist.TH

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
    doInsert ((outcropId, insertedAt) : xs) (success, failed) = do
      insertResult <- insert $ AuditLog "outcrop" "created" 11 (Just outcropId) Nothing insertedAt
      if insertResult == toSqlKey 0
        then doInsert xs (success + 1, failed)
        else doInsert xs (success, failed + 1)
