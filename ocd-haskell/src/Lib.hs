{-# LANGUAGE OverloadedStrings #-}

module Lib (parseCsvFile) where

import qualified Data.ByteString.Lazy as BL
import Data.Csv
import Data.Time (LocalTime, defaultTimeLocale, parseTimeM)
import qualified Data.Vector as V

data OcInfo = OcInfo {outcropId :: Int, creationDate :: LocalTime} deriving (Show)

instance FromNamedRecord OcInfo where
  parseNamedRecord m =
    OcInfo
      <$> m .: "ID"
      <*> (parseDate =<< m .: "Inserted At")
   where
    parseDate :: String -> Parser LocalTime
    parseDate = parseTimeM True defaultTimeLocale "%Y-%m-%d %T"

parseCsvFile :: IO ()
parseCsvFile = do
  csvData <- BL.readFile "Outcrop_creationDates_.InsertedAt.csv"
  case decodeByNameWith decodeOpts csvData :: Either String (Header, V.Vector OcInfo) of
    Left err -> putStrLn $ "Error: " ++ err
    Right (_, v) -> V.forM_ v print
 where
  decodeOpts = defaultDecodeOptions{decDelimiter = fromIntegral (fromEnum ';')}
