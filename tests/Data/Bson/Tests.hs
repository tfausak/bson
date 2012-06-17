{-# LANGUAGE TypeSynonymInstances #-}

module Data.Bson.Tests
    ( tests
    ) where

import Control.Applicative ((<$>))
import Data.Int (Int32, Int64)
import Data.Time.Calendar (Day(ModifiedJulianDay))
import Data.Time.Clock.POSIX (POSIXTime)
import Data.Time.Clock (UTCTime(..), addUTCTime)

import Data.Text (Text)
import Test.Framework (Test, testGroup)
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Test.QuickCheck (Arbitrary(..))
import qualified Data.Text as T

import Data.Bson (Val(cast', val))

instance Arbitrary Text where
    arbitrary = T.pack <$> arbitrary

instance Arbitrary POSIXTime where
    arbitrary = fromInteger <$> arbitrary

instance Arbitrary UTCTime where
    arbitrary = do
        a <- arbitrary
        b <- arbitrary
        return $ addUTCTime (fromRational b)
               $ UTCTime (ModifiedJulianDay a) 0

tests :: Test
tests = testGroup "Data.Bson.Tests"
    [ testProperty "Val Bool"      (testVal :: Bool -> Bool)
    , testProperty "Val Double"    (testVal :: Double -> Bool)
    , testProperty "Val Float"     (testVal :: Float -> Bool)
    , testProperty "Val Int"       (testVal :: Int -> Bool)
    , testProperty "Val Int32"     (testVal :: Int32 -> Bool)
    , testProperty "Val Int64"     (testVal :: Int64 -> Bool)
    , testProperty "Val Integer"   (testVal :: Integer -> Bool)
    , testProperty "Val String"    (testVal :: String -> Bool)
    , testProperty "Val POSIXTime" (testVal :: POSIXTime -> Bool)
    , testProperty "Val UTCTime"   (testVal :: UTCTime -> Bool)
    , testProperty "Val Text"      (testVal :: Text -> Bool)
    ]

testVal :: Val a => a -> Bool
testVal a = case cast' . val $ a of
    Nothing -> False
    Just a' -> a == a'