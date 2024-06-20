module OCD (backfill) where

import Data.List (intercalate)
import OCD.CSV
import OCD.Database

backfill :: IO ()
backfill = do
  parseResult <- parseCsvFile "dates.csv"
  case parseResult of
    Left err -> putStrLn $ "Error parsing CSV:" ++ err
    Right ocInfo -> do
      ocIds <- ocIdsWithExistingLogs $ map outcropId ocInfo
      print $ intercalate ", " $ map show ocIds
