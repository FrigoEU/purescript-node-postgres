module Database.Postgres.SqlValue
  ( SqlValue()
  , class IsSqlValue
  , toSql
  , fromSql
  , readSqlProp
  ) where

import Prelude
import Data.Array (index, (!!))
import Data.Date (Day, Month, Year, Date, canonicalDate, year, month, day)
import Data.DateTime (DateTime(DateTime))
import Data.Either (Either(Right, Left))
import Data.Enum (toEnum, fromEnum)
import Data.Foreign (ForeignError(TypeMismatch), Foreign, F)
import Data.Foreign.Class (read)
import Data.Foreign.Index (class Index, (!))
import Data.Foreign.Null (readNull, Null, unNull)
import Data.Int (fromString, toNumber)
import Data.Maybe (maybe, Maybe)
import Data.Nullable (toNullable)
import Data.String (split)
import Data.Time (Second, Minute, Hour, Time(Time), second, minute, hour)
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

instance isSqlValueDateTime :: IsSqlValue DateTime where
  toSql (DateTime d t) = toSql (dateToString d <> " " <> timeToString t)

  fromSql o = (read o :: F String) >>= parse
    where 
      parse str = do
        let dts = split " " str
        ds <- maybe (Left $ TypeMismatch "Expected Datetime" "Didn't find Date component") Right $ index dts 0
        ts <- maybe (Left $ TypeMismatch "Expected Datetime" "Didn't find Time component") Right $ index dts 1

        d :: Date <- dateFromString ds
        t :: Time <- timeFromString ts
        pure $ DateTime d t


instance isSqlValueDate :: IsSqlValue Date where
  toSql = toSql <<< dateToString
  fromSql ds = read ds >>= dateFromString

dateToString :: Date -> String
dateToString d = show (fromEnum (year d)) <> "-"
                 <> zeroPad (fromEnum (month d)) <> "-"
                 <> zeroPad (fromEnum (day d))

dateFromString :: String -> Either ForeignError Date
dateFromString dstr = do
  let dsplit = split "-" dstr
  ys <- maybe (Left $ TypeMismatch "Expected Year" "Didn't find Year string") Right $ index dsplit 0
  y :: Year <- parseInt ys >>= (toEnum >>> maybe (Left $ TypeMismatch "Expected Year" ys) Right)
  ms <- maybe (Left $ TypeMismatch "Expected Month" "Didn't find Month string") Right $ index dsplit 1
  m :: Month <- parseInt ms >>= (toEnum >>> maybe (Left $ TypeMismatch "Expected Month" ms) Right)
  dateS <- maybe (Left $ TypeMismatch "Expected Day" "Didn't find Day string") Right $ index dsplit 2
  date :: Day <- parseInt dateS >>= (toEnum >>> maybe (Left $ TypeMismatch "Expected Day" dateS) Right)
  pure $ canonicalDate y m date

instance isSqlValueTime :: IsSqlValue Time where
  toSql = toSql <<< timeToString
  fromSql ts = read ts >>= timeFromString

timeToString :: Time -> String
timeToString t = zeroPad (fromEnum (hour t)) <> ":"
                 <> zeroPad (fromEnum (minute t)) <> ":"
                 <> zeroPad (fromEnum (second t))

timeFromString :: String -> Either ForeignError Time
timeFromString tstr = do
  let tsplit = split ":" tstr
  hs <- maybe (Left $ TypeMismatch "Expected Hour" "Didn't find Hour string") Right $ index tsplit 0
  h :: Hour <- parseInt hs >>= (toEnum >>> maybe (Left $ TypeMismatch "Expected Hour" hs) Right)
  ms <- maybe (Left $ TypeMismatch "Expected Minutes" "Didn't find Minutes string") Right $ index tsplit 1
  m :: Minute <- parseInt ms >>= (toEnum >>> maybe (Left $ TypeMismatch "Expected Minutes" ms) Right)
  ss <- maybe (Left $ TypeMismatch "Expected Seconds" "Didn't find Seconds string") Right $ index tsplit 2
  s :: Second <- parseInt ss >>= (toEnum >>> maybe (Left $ TypeMismatch "Expected Seconds" ss) Right)
  pure $ Time h m s bottom

zeroPad :: Int -> String
zeroPad i | i < 10 = "0" <> (show i)
zeroPad i = show i

parseInt :: String -> F Int
parseInt i = maybe (Left $ TypeMismatch "Expected Int" i) Right $ fromString i

readSqlProp :: forall a b. (Index b, IsSqlValue a) => b -> Foreign -> Either ForeignError a
readSqlProp prop value = value ! prop >>= fromSql
