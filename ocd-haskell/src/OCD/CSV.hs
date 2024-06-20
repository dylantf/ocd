{-# LANGUAGE OverloadedStrings #-}

module OCD.CSV (parseCsvFile, OcInfo, outcropId, insertedAt) where

import qualified Data.ByteString.Lazy as BL
import Data.Csv
import Data.Int (Int64)
import Data.Time (LocalTime, defaultTimeLocale, parseTimeM)
import qualified Data.Vector as V

data OcInfo = OcInfo {outcropId :: Int64, insertedAt :: LocalTime} deriving (Show)

instance FromNamedRecord OcInfo where
  parseNamedRecord m =
    OcInfo
      <$> m .: "ID"
      <*> (parseDate =<< m .: "Inserted At")
    where
      parseDate :: String -> Parser LocalTime
      parseDate = parseTimeM True defaultTimeLocale "%Y-%m-%d %T"

parseCsvFile :: String -> IO (Either String [OcInfo])
parseCsvFile filePath = do
  csvData <- BL.readFile filePath
  case decodeByNameWith decodeOpts csvData :: Either String (Header, V.Vector OcInfo) of
    Left err -> pure $ Left err
    Right (_, v) -> pure $ Right (V.toList v)
  where
    decodeOpts = defaultDecodeOptions {decDelimiter = fromIntegral (fromEnum ';')}
