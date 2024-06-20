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

module OCD.Database (loadOutcrops) where

import Control.Monad.IO.Class
import Control.Monad.Logger (runStdoutLoggingT)
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
    outcropId Int Maybe
    studyId Int Maybe
    insertedAt UTCTime
    deriving Show
|]

connString :: ConnectionString
connString = "postgresql://postgres:postgres@localhost:5432/safari_api"

loadOutcrops :: IO ()
loadOutcrops = runStdoutLoggingT $ withPostgresqlConn connString $ \backend -> liftIO $ do
  flip runSqlConn backend $ do
    logs <- selectList [] [] :: SqlPersistT IO [Entity AuditLog]
    liftIO $ mapM_ (print . entityVal) logs
