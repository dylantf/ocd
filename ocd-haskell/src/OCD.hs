module OCD (backfill) where

import OCD.CSV
import OCD.Database

backfill :: IO ()
backfill = do
  parseResult <- parseCsvFile "dates.csv"
  case parseResult of
    Left err -> putStrLn $ "Error parsing CSV:" ++ err
    Right ocInfo -> mapM_ print ocInfo
  loadOutcrops