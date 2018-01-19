module Database.Postgres
  ( Query(..)
  , Client()
  , DB()
  , ConnectionInfo()
  , ConnectionString()
  , mkConnectionString
  , connect
  , disconnect
  , end
  , execute, execute_
  , query, query_
  , queryValue, queryValue_
  , queryOne, queryOne_
  , withConnection
  , withClient
  , withClient'
  ) where

import Prelude

import Control.Monad.Aff (Aff, finally)
import Control.Monad.Eff (Eff, kind Effect)
import Control.Monad.Eff.Class (liftEff)
import Control.Monad.Eff.Exception (Error, error)
import Control.Monad.Error.Class (throwError)
import Control.Monad.Except (runExcept)
import Data.Array ((!!))
import Data.Bifunctor (lmap)
import Data.Either (Either, either)
import Data.Foreign (Foreign, ForeignError)
import Data.Function.Uncurried (Fn2, runFn2)
import Data.Maybe (Maybe(Just, Nothing), maybe)
import Data.Newtype (unwrap)
import Data.NonEmpty (head)
import Data.Traversable (sequence)
import Database.Postgres.SqlValue (fromSql, class IsSqlValue, SqlValue)

newtype Query a = Query String

foreign import data Client :: Type

foreign import data DB :: Effect

type ConnectionString = String

type ConnectionInfo =
  { host :: String
  , db :: String
  , port :: Int
  , user :: String
  , password :: String
  , ssl :: Boolean
  }

foreign import data Field :: Type
type RawResult = { rows :: Array Foreign }

mkConnectionString :: ConnectionInfo -> ConnectionString
mkConnectionString ci =
    "postgres://"
  <> ci.user <> ":"
  <> ci.password <> "@"
  <> ci.host <> ":"
  <> show ci.port <> "/"
  <> ci.db
  <> "?ssl=" <> show ci.ssl

-- | Makes a connection to the database.
connect :: forall eff. ConnectionInfo -> Aff (db :: DB | eff) Client
connect = connect' <<< mkConnectionString

-- | Runs a query and returns nothing.
execute :: forall eff a. Query a -> Array SqlValue -> Client -> Aff (db :: DB | eff) Unit
execute (Query sql) params client = void $ runQuery sql params client

-- | Runs a query and returns nothing
execute_ :: forall eff a. Query a -> Client -> Aff (db :: DB | eff) Unit
execute_ (Query sql) client = void $ runQuery_ sql client

foreign import showDiagnostics :: RawResult -> Foreign -> String

processRowWithDiagnostics :: forall a. (IsSqlValue a) => RawResult -> Foreign -> Either Error a
processRowWithDiagnostics res row = 
  fromSql row # runExcept # lmap (unwrap >>> head >>> addDiagnostics)
    where 
      addDiagnostics e = error $ show e <> "\n. Postgres Diagnostics: " <> showDiagnostics res row

-- | Runs a query and returns all results.
query :: forall eff a
  . (IsSqlValue a)
  => Query a -> Array SqlValue -> Client -> Aff (db :: DB | eff) (Array a)
query (Query sql) params client = do
  res <- runQuery sql params client
  either throwError pure (sequence $ processRowWithDiagnostics res <$> res.rows)
  -- I suppose the only thing I can do here is take the first error?

-- | Just like `query` but does not make any param replacement
query_ :: forall eff a. (IsSqlValue a) => Query a -> Client -> Aff (db :: DB | eff) (Array a)
query_ (Query sql) client = do
  res <- runQuery_ sql client
  either throwError pure (sequence $ processRowWithDiagnostics res <$> res.rows)

-- | Runs a query and returns the first row, if any
queryOne :: forall eff a
  . (IsSqlValue a)
  => Query a -> Array SqlValue -> Client -> Aff (db :: DB | eff) (Maybe a)
queryOne (Query sql) params client = do
  res <- runQuery sql params client
  maybe (pure Nothing) (either throwError pure) $ processRowWithDiagnostics res <$> (getFirstRealRow res.rows)

foreign import isObjectWithAllNulls :: Foreign -> Boolean
getFirstRealRow :: Array Foreign -> Maybe Foreign
getFirstRealRow rows =
    (rows !! 0) >>= \r -> if isObjectWithAllNulls r then Nothing else Just r


-- | Just like `queryOne` but does not make any param replacement
queryOne_ :: forall eff a. (IsSqlValue a) => Query a -> Client -> Aff (db :: DB | eff) (Maybe a)
queryOne_ (Query sql) client = do
  res <- runQuery_ sql client
  maybe (pure Nothing) (either throwError pure) $ processRowWithDiagnostics res <$> (getFirstRealRow res.rows)

-- | Runs a query and returns a single value, if any.
queryValue :: forall eff a
  . (IsSqlValue a)
  => Query a -> Array SqlValue -> Client -> Aff (db :: DB | eff) (Maybe a)
queryValue (Query sql) params client = do
  val <- runQueryValue sql params client
  pure $ either (const Nothing) Just (runExcept $ fromSql val)

-- | Just like `queryValue` but does not make any param replacement
queryValue_ :: forall eff a. (IsSqlValue a) => Query a -> Client -> Aff (db :: DB | eff) (Maybe a)
queryValue_ (Query sql) client = do
  val <- runQueryValue_ sql client
  either (unwrap >>> head >>> liftError) pure $ runExcept $ fromSql val

-- | Connects to the database, calls the provided function with the client
-- | and returns the results.
withConnection :: forall eff a
  . ConnectionInfo
  -> (Client -> Aff (db :: DB | eff) a)
  -> Aff (db :: DB | eff) a
withConnection info p = do
  client <- connect info
  finally (liftEff (end client)) (p client)

-- | Takes a Client from the connection pool, runs the given function with
-- | the client and returns the results.
withClient :: forall eff a
  . ConnectionInfo
  -> (Client -> Aff (db :: DB | eff) a)
  -> Aff (db :: DB | eff) a
withClient info p = runFn2 _withClient (mkConnectionString info) p

withClient' :: forall eff a
  . ConnectionString
  -> (Client -> Aff ( db :: DB | a) eff)
  -> Aff ( db :: DB | a) eff
withClient' = runFn2 _withClient

liftError :: forall e a. ForeignError -> Aff e a
liftError err = throwError $ error (show err)

foreign import connect' :: forall eff. String -> Aff (db :: DB | eff) Client

foreign import _withClient :: forall eff a. Fn2 ConnectionString (Client -> Aff (db :: DB | eff) a) (Aff (db :: DB | eff) a)

foreign import runQuery_ :: forall eff. String -> Client -> Aff (db :: DB | eff) RawResult

foreign import runQuery :: forall eff. String -> Array SqlValue -> Client -> Aff (db :: DB | eff) RawResult

foreign import runQueryValue_ :: forall eff. String -> Client -> Aff (db :: DB | eff) Foreign

foreign import runQueryValue :: forall eff. String -> Array SqlValue -> Client -> Aff (db :: DB | eff) Foreign

foreign import end :: forall eff. Client -> Eff (db :: DB | eff) Unit

foreign import disconnect :: forall eff. Eff (db :: DB | eff) Unit
