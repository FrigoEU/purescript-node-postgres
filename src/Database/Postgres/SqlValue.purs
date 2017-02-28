module Database.Postgres.SqlValue
  ( SqlValue()
  , class IsSqlValue
  , toSql
  , fromSql
  , readSqlProp
  ) where

import Prelude
import Control.Monad.Except (except)
import Data.Array ((!!))
import Data.Date (Day, Month, Year, Date, canonicalDate, year, month, day)
import Data.DateTime (DateTime(DateTime))
import Data.Either (Either(Right, Left), fromRight)
import Data.Enum (toEnum, fromEnum)
import Data.Foreign (F, Foreign, ForeignError(..), fail, isArray)
import Data.Foreign.Class (read)
import Data.Foreign.Index (class Index, (!))
import Data.Foreign.Null (readNull, Null, unNull)
import Data.Int (fromString, toNumber)
import Data.Maybe (maybe, Maybe)
import Data.Nullable (toNullable)
import Data.String (Pattern(Pattern), split)
import Data.String.Regex (regex)
import Data.String.Regex (split) as R
import Data.String.Regex.Flags (global, ignoreCase)
import Data.Time (Second, Minute, Hour, Time(Time), second, minute, hour)
import Data.Time.Duration (Hours(..), Minutes(..))
import Data.Traversable (traverse)
import Partial.Unsafe (unsafePartial)
import Unsafe.Coerce (unsafeCoerce)

foreign import data SqlValue :: *

class IsSqlValue a where
  toSql :: a -> SqlValue
  fromSql :: Foreign -> F a

instance isSqlValueString :: IsSqlValue String where
  toSql = unsafeCoerce
  fromSql = read

instance isSqlValueNumber :: IsSqlValue Number where
  toSql = unsafeCoerce
  fromSql = read

instance isSqlValueInt :: IsSqlValue Int where
  toSql = unsafeCoerce <<< toNumber
  fromSql = read

instance isSqlValueBoolean :: IsSqlValue Boolean where
  toSql = unsafeCoerce
  fromSql = read

instance isSqlValueMaybe :: (IsSqlValue a) => IsSqlValue (Maybe a) where
  toSql = unsafeCoerce <<< toNullable <<< (toSql <$> _)
  fromSql s = unNull <$> (readNull fromSql s :: F (Null a))

instance isSqlValueArray :: (IsSqlValue a) => IsSqlValue (Array a) where
  toSql = unsafeCoerce <<< (toSql <$> _)
  fromSql s = if isArray s then traverse fromSql (unsafeCoerce s :: Array Foreign)
                           else (fail $ TypeMismatch "Expected array" "Didn't find array")

instance isSqlValueDateTime :: IsSqlValue DateTime where
  toSql (DateTime d t) = toSql (dateToString d <> " " <> timeToString t)

  fromSql o = (read o :: F String) >>= parse
    where
      parse str = do
        let dts = split (Pattern " ") str
        ds <- maybe (fail $ TypeMismatch "Expected Datetime" "Didn't find Date component") pure (dts !! 0)
        ts <- maybe (fail $ TypeMismatch "Expected Datetime" "Didn't find Time component") pure (dts !! 1)

        d :: Date <- dateFromString ds
        t :: Time <- timeFromString ts
        pure $ DateTime d t


instance isSqlValueDate :: IsSqlValue Date where
  toSql = toSql <<< dateToString
  fromSql ds = read ds >>= dateFromString

derive newtype instance isSqlValueMinutes :: IsSqlValue Minutes
derive newtype instance isSqlValueHours :: IsSqlValue Hours

dateToString :: Date -> String
dateToString d = show (fromEnum (year d)) <> "-"
                 <> zeroPad (fromEnum (month d)) <> "-"
                 <> zeroPad (fromEnum (day d))

dateFromString :: String -> F Date
dateFromString dstr = do
  let dsplit = split (Pattern "-") dstr
  ys <- maybe (fail $ TypeMismatch "Expected Year" "Didn't find Year string") pure (dsplit !! 0)
  y :: Year <- parseInt ys >>= (toEnum >>> maybe (fail $ TypeMismatch "Expected Year" ys) pure)
  ms <- maybe (fail $ TypeMismatch "Expected Month" "Didn't find Month string") pure (dsplit !! 1)
  m :: Month <- parseInt ms >>= (toEnum >>> maybe (fail $ TypeMismatch "Expected Month" ms) pure)
  dateS <- maybe (fail $ TypeMismatch "Expected Day" "Didn't find Day string") pure (dsplit !! 2)
  date :: Day <- parseInt dateS >>= (toEnum >>> maybe (fail $ TypeMismatch "Expected Day" dateS) pure)
  pure $ canonicalDate y m date

instance isSqlValueTime :: IsSqlValue Time where
  toSql = toSql <<< timeToString
  fromSql ts = read ts >>= timeFromString

timeToString :: Time -> String
timeToString t = zeroPad (fromEnum (hour t)) <> ":"
                 <> zeroPad (fromEnum (minute t)) <> ":"
                 <> zeroPad (fromEnum (second t))

timeFromString :: String -> F Time
timeFromString tstr = do
  let tsplit = R.split (unsafePartial (fromRight (regex "[:\\.]" (global <> ignoreCase)))) tstr
  hs <- maybe (fail $ TypeMismatch "Expected Hour" "Didn't find Hour string") pure (tsplit !! 0)
  h :: Hour <- parseInt hs >>= (toEnum >>> maybe (fail $ TypeMismatch "Expected Hour" hs) pure)
  ms <- maybe (fail $ TypeMismatch "Expected Minutes" "Didn't find Minutes string") pure (tsplit !! 1)
  m :: Minute <- parseInt ms >>= (toEnum >>> maybe (fail $ TypeMismatch "Expected Minutes" ms) pure)
  ss <- maybe (fail $ TypeMismatch "Expected Seconds" "Didn't find Seconds string") pure (tsplit !! 2)
  s :: Second <- parseInt ss >>= (toEnum >>> maybe (fail $ TypeMismatch "Expected Seconds" ss) pure)
  pure $ Time h m s bottom

zeroPad :: Int -> String
zeroPad i | i < 10 = "0" <> (show i)
zeroPad i = show i

parseInt :: String -> F Int
parseInt i = except $ maybe (Left $ pure $ TypeMismatch "Expected Int" i) Right $ fromString i

readSqlProp :: forall a b. (Index b, IsSqlValue a) => b -> Foreign -> F a
readSqlProp prop value = value ! prop >>= fromSql
