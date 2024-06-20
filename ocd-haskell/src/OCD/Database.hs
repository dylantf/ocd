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

module OCD.Database (ocIdsWithExistingLogs) where

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
    userId Int
    outcropId Int64 Maybe
    studyId Int64 Maybe
    insertedAt UTCTime
    deriving Show
|]

ocIdsWithExistingLogs :: [Int64] -> IO [Int64]
ocIdsWithExistingLogs outcropIds = runStdoutLoggingT $ withPostgresqlConn connString $ \backend -> liftIO $ do
  flip runSqlConn backend $ do
    auditLogs <-
      selectList
        [ AuditLogEntity ==. "outcrop",
          AuditLogOutcropId <-. map Just outcropIds
        ]
        []
    return $ mapMaybe (auditLogOutcropId . entityVal) auditLogs
  where
    connString = "postgresql://postgres:postgres@localhost:5432/safari_api"
