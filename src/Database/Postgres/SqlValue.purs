module Database.Postgres.SqlValue
  ( SqlValue()
  , class IsSqlValue
  , toSql
  , fromSql
  , readSqlProp
  , encodeJsonInSql
  , decodeJsonFromSql
  ) where

import Prelude

import Control.Monad.Error.Class (throwError)
import Control.Monad.Except (except)
import Data.Argonaut.Core (stringify)
import Data.Argonaut.Decode (class DecodeJson, decodeJson)
import Data.Argonaut.Encode (class EncodeJson, encodeJson)
import Data.Argonaut.Parser (jsonParser)
import Data.Array ((!!))
import Data.Date (Day, Month, Year, Date, canonicalDate, year, month, day)
import Data.DateTime (DateTime(DateTime))
import Data.Either (Either(Right, Left), either, fromRight)
import Data.Enum (toEnum, fromEnum)
import Data.Foreign (F, Foreign, ForeignError(ForeignError, TypeMismatch), fail, isArray, readBoolean, readInt, readNull, readNumber, readString)
import Data.Foreign.Index (class Index, (!))
import Data.Int (fromString, toNumber)
import Data.List.NonEmpty (singleton)
import Data.Maybe (Maybe, maybe)
import Data.Nullable (toNullable)
import Data.Number (fromString) as N
import Data.String (Pattern(Pattern), split)
import Data.String.Regex (regex)
import Data.String.Regex (split) as R
import Data.String.Regex.Flags (global, ignoreCase)
import Data.Time (Second, Minute, Hour, Time(Time), second, minute, hour)
import Data.Time.Duration (Hours(..), Milliseconds(..), Minutes(..))
import Data.Traversable (sequence, traverse)
import Data.Tuple (Tuple(..))
import Partial.Unsafe (unsafePartial)
import Unsafe.Coerce (unsafeCoerce)

foreign import data SqlValue :: Type

class IsSqlValue a where
  toSql :: a -> SqlValue
  fromSql :: Foreign -> F a

instance isSqlValueString :: IsSqlValue String where
  toSql = unsafeCoerce
  fromSql = readString

instance isSqlValueNumber :: IsSqlValue Number where
  toSql = unsafeCoerce
  fromSql = readNumber

instance isSqlValueInt :: IsSqlValue Int where
  toSql = unsafeCoerce <<< toNumber
  fromSql = readInt

instance isSqlValueBoolean :: IsSqlValue Boolean where
  toSql = unsafeCoerce
  fromSql = readBoolean

instance isSqlValueMaybe :: (IsSqlValue a) => IsSqlValue (Maybe a) where
  toSql as = unsafeCoerce (toNullable (toSql <$> as))
  fromSql = readNull >=> map fromSql >>> sequence

instance isSqlValueArray :: (IsSqlValue a) => IsSqlValue (Array a) where
  toSql as = unsafeCoerce (toSql <$> as)
  fromSql s = if isArray s then traverse fromSql (unsafeCoerce s :: Array Foreign)
                           else (fail $ TypeMismatch "Expected array" "Didn't find array")

instance isSqlValueDateTime :: IsSqlValue DateTime where
  toSql (DateTime d t) = toSql (dateToString d <> " " <> timeToString t)

  fromSql o = readString o >>= parse
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
  fromSql ds = readString ds >>= dateFromString

instance isSqlValueMinutes :: IsSqlValue Minutes where
  toSql (Minutes m) = toSql m
  fromSql ds = readString ds >>= parseNumber <#> Minutes

instance isSqlValueMilliSeconds :: IsSqlValue Milliseconds where
  toSql (Milliseconds m) = toSql m
  fromSql ds = readString ds >>= parseNumber <#> Milliseconds

instance isSqlValueHours :: IsSqlValue Hours where
  toSql (Hours m) = toSql m
  fromSql ds = readNumber ds <#> Hours

instance isSqlValueSqlValue :: IsSqlValue SqlValue where
  toSql = id
  fromSql = unsafeCoerce >>> pure

instance isSqlValueForeign :: IsSqlValue Foreign where
  toSql = unsafeCoerce
  fromSql = pure

unsafeForeignToSqlValue :: Foreign -> SqlValue
unsafeForeignToSqlValue = unsafeCoerce

instance isSqlValueTuple :: (IsSqlValue a, IsSqlValue b) =>
                            IsSqlValue (Tuple a b) where
  toSql (Tuple a b) = toSql [toSql a, toSql b]
  fromSql s = do
      arr <- fromSql s :: F (Array Foreign)
      a <- maybe (fail $ ForeignError "Expected First element of tuple") fromSql (arr !! 0)
      b <- maybe (fail $ ForeignError "Expected Second element of tuple") fromSql (arr !! 1)
      pure (Tuple a b)

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
  fromSql ts = readString ts >>= timeFromString

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

parseNumber :: String -> F Number
parseNumber i = except $ maybe (Left $ pure $ TypeMismatch "Expected Number" i) Right $ N.fromString i

readSqlProp :: forall a b. (Index b) => (IsSqlValue a) => b -> Foreign -> F a
readSqlProp prop value = value ! prop >>= fromSql

encodeJsonInSql :: forall a. (EncodeJson a) => a -> SqlValue
encodeJsonInSql = encodeJson >>> stringify >>> toSql
decodeJsonFromSql :: forall a. (DecodeJson a) => Foreign -> F a
decodeJsonFromSql a = readString a >>= \str -> either (ForeignError >>> singleton >>> throwError) pure (jsonParser str >>= decodeJson)
