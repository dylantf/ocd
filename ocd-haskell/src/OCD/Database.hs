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
  auditLogs <-
    selectList
      [AuditLogEntity ==. "outcrop", AuditLogOutcropId <-. map Just outcropIds]
      []
  return $ mapMaybe (auditLogOutcropId . entityVal) auditLogs

insertNewLogs :: [(Int64, UTCTime)] -> IO (Int, Int)
insertNewLogs toBeCreated = doInsert toBeCreated (0, 0)
  where
    doInsert :: [(Int64, UTCTime)] -> (Int, Int) -> IO (Int, Int)
    doInsert [] result = return result
    doInsert ((outcropId, insertedAt) : xs) (success, failed) = do
      _ <- execQuery $ do
        insert $
          AuditLog
            { auditLogEntity = "outcrop",
              auditLogAction = "created",
              auditLogUserId = 11,
              auditLogOutcropId = Just outcropId,
              auditLogStudyId = Nothing,
              auditLogInsertedAt = insertedAt
            }
      doInsert xs (success + 1, failed)