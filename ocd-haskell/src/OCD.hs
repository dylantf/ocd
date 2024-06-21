module OCD (backfill) where

import OCD.CSV
import OCD.Database

backfill :: IO ()
backfill = do
  parseResult <- parseCsvFile "dates.csv"
  case parseResult of
    Left err -> putStrLn $ "Error parsing CSV:" ++ err
    Right ocInfos -> do
      ocIds <- ocIdsWithExistingLogs $ map outcropId ocInfos
      putStrLn $ "Found " ++ show (length ocIds) ++ " existing OCs with logs"
      let toInsert = filter (\ocInfo -> outcropId ocInfo `elem` ocIds) ocInfos
      putStrLn $ show (length toInsert) ++ " records to be created"
      (success, failed) <- insertNewLogs $ map (\ocInfo -> (outcropId ocInfo, insertedAt ocInfo)) ocInfos
      putStrLn $ "Created " ++ show success ++ " successful logs with " ++ show failed ++ " failed."