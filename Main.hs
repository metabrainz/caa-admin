{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}
module Main where

import Prelude hiding (mapM_)

import Control.Applicative ()
import Control.Lens hiding ((|>))
import Control.Monad (join)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Reader.Class (asks)
import Control.Monad.State.Class (get, modify, put)
import Control.Monad.Trans.Maybe (MaybeT(..))
import Data.Foldable (forM_, mapM_)
import Data.Maybe (fromMaybe)
import Data.Monoid ()
import Data.Text (Text)
import Data.Time (UTCTime, getCurrentTime)
import Data.Typeable (Typeable)
import Network.Socket.Internal ()
import Text.Blaze ((!))

import qualified Data.Acid as AcidState
import qualified Data.Configurator as Config
import qualified Data.Configurator.Types as Config
import qualified Data.IntMap as IntMap
import qualified Data.Map as Map
import qualified Data.SafeCopy as SafeCopy
import qualified Data.Sequence as Sequence
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Network.AMQP as AMQP
import qualified Network.AMQP.Types as AMQP
import qualified Snap
import qualified Snap.Snaplet.AcidState as SnapletAcidState
import qualified Text.Blaze.Html as Blaze
import qualified Text.Blaze.Html.Renderer.Utf8 as Blaze
import qualified Text.Blaze.Html5 as H
import qualified Text.Blaze.Html5.Attributes as A

--------------------------------------------------------------------------------
-- | The possible type of event that the CAA-indexer should be able to process.
data EventType = Index | Delete | Move
  deriving (Show)

instance H.ToMarkup EventType where
  toMarkup = H.toMarkup . show

-- | An event that could not be processed by the CAA-indexer due to exceptional
-- circumstances.
data FailedEvent = FailedEvent
    { failureReasons :: Sequence.Seq Text
    -- ^ An ordered list of exceptions that were encountered at each attempt to
    -- process this event.
    , observedFailureAt :: UTCTime
    -- ^ The time *this application* observed the failure
    , failedEventType :: EventType
    -- ^ The type of event
    , failedEventBody :: Text
    -- ^ The event payload
    , failedEventId :: Int
    -- ^ An unique identifier for this event, that is internal to this
    -- application.
    }
  deriving (Show, Typeable)

-- | The state of this application is an 'IntMap', sending integers to
-- 'FailedEvent's; and also a single Int specifying the next 'failedEventId'. We
-- track this separately, in order to ensure that 'failedEventId' is unique.
data FailedEvents = FailedEvents (IntMap.IntMap FailedEvent) Int
  deriving (Typeable)

--------------------------------------------------------------------------------
-- | Return a list of all 'FailedEvent's that have not yet been handled. Ordered
-- from oldest to newest.
allFailedEvents :: AcidState.Query FailedEvents [FailedEvent]
allFailedEvents = asks $ \(FailedEvents m _) -> IntMap.elems m

-- | Append a new failed event to the list of known failed events.
appendFailedEvent
  :: Sequence.Seq Text -> UTCTime -> EventType -> Text
  -> AcidState.Update FailedEvents ()
appendFailedEvent reasons time evType body =
  modify $ \(FailedEvents events i) ->
    let event = FailedEvent reasons time evType body newId
        newId = i + 1
    in FailedEvents (IntMap.insert newId event events) newId

-- | Delete and select 'FailedEvents' by their 'failedEventId'. All successfully
-- removed 'FailedEvent's are returned. If an ID does not exist in
-- 'FailedEvents', then that ID is discarded.
deleteEvents :: [Int] -> AcidState.Update FailedEvents [FailedEvent]
deleteEvents ids = do
  FailedEvents m nextId <- get
  let (deleted, rest) = IntMap.partitionWithKey (\i _ -> i`elem` ids) m
  IntMap.elems deleted <$ put (FailedEvents rest nextId)

SafeCopy.deriveSafeCopy 0 'SafeCopy.base ''EventType
SafeCopy.deriveSafeCopy 0 'SafeCopy.base ''FailedEvent
SafeCopy.deriveSafeCopy 0 'SafeCopy.base ''FailedEvents
AcidState.makeAcidic ''FailedEvents ['allFailedEvents, 'appendFailedEvent, 'deleteEvents]

--------------------------------------------------------------------------------
-- | The state of the entire CAA-admin application, as required by Snap.
data CaaAdmin = CaaAdmin
    { _acid :: Snap.Snaplet (SnapletAcidState.Acid FailedEvents)
    -- ^ A snaplet-acid-state to the applications state.
    , retryMessage :: FailedEvent -> IO ()
    -- ^ A function that attempts to retry a 'FailedEvent'. This is part of the
    -- application state, as the behaviour of this function depends on
    -- parameters in the configuration files.
    }
makeLenses ''CaaAdmin

instance SnapletAcidState.HasAcid CaaAdmin FailedEvents where
  getAcidStore = view (acid . Snap.snapletValue)

-- | Initialize the CAA-admin web site by loading the previous state from
-- a local file (if possible), and connect to RabbitMQ.
initCaaAdmin :: Snap.SnapletInit CaaAdmin CaaAdmin
initCaaAdmin =
  Snap.makeSnaplet "caa-admin" "Cover Art Archive administration" Nothing $ do
    acidState <- liftIO (AcidState.openLocalState $ FailedEvents mempty 0)
    Snap.onUnload (AcidState.closeAcidState acidState)

    acidStateSnaplet <- Snap.embedSnaplet "" acid $
      SnapletAcidState.acidInitManual acidState

    caaMqConfig <- Config.subconfig "cover-art-archive" <$> Snap.getSnapletUserConfig
    let caaOption :: Config.Configured a => Text -> a -> IO a
        caaOption k def = Config.lookupDefault def caaMqConfig k

    rabbitConn <- liftIO $
      join $ AMQP.openConnection' <$> caaOption "host" "127.0.0.1"
                                  <*> ((caaOption "port" 5672) >>= \x -> return $ fromInteger x)
                                  <*> caaOption "vhost" "/cover-art-archive"
                                  <*> caaOption "username" "guest"
                                  <*> caaOption "password" "guest"

    failureQueue <- liftIO (caaOption "failure-queue" "cover-art-archive.failed")
    stopListening <- liftIO . consumeFailures rabbitConn failureQueue $ acidState
    Snap.onUnload $ stopListening >> AMQP.closeConnection rabbitConn

    Snap.addRoutes
      [ ("/", showFailures)
      , ("/retry", retry)
      ]

    retryChan <- liftIO (AMQP.openChannel rabbitConn)
    retryExchange <- liftIO (caaOption "retry-exchange" "cover-art-archive")
    return $ CaaAdmin acidStateSnaplet (mkRetryEvent retryChan retryExchange)

 where
   mkRetryEvent chan exchange event =
     AMQP.publishMsg chan exchange
       (routingKey $ failedEventType event)
       AMQP.newMsg { AMQP.msgBody = view (from strict) . Text.encodeUtf8 $ failedEventBody event }

   routingKey Index = "index"
   routingKey Move = "move"
   routingKey Delete = "delete"

--------------------------------------------------------------------------------
-- | Present a table of all known failed events, and add a form that allows the
-- user to retry selected events.
showFailures :: Snap.Handler CaaAdmin CaaAdmin ()
showFailures = do
  events <- SnapletAcidState.query AllFailedEvents
  blaze $ H.html $ do
    H.head $ H.title "Cover Art Archive Administration"
    H.body $ do
      H.h1 "Failed Events"
      if null events
        then H.p "These are not the failed events you are looking for; nothing is wrong!"
        else
          H.form ! A.action "/retry" ! A.method "POST" $ do
            H.table $ do
              H.thead $
                H.tr $ do
                  H.th mempty
                  H.th "Failed At"
                  H.th "Type"
                  H.th "Exceptions"
                  H.th "Message Body"
              H.tbody $
                forM_ events $ \event -> H.tr $ do
                  H.td $ H.input ! A.type_ "checkbox" ! A.name "event_id"
                                 ! A.value (Blaze.toValue $ failedEventId event)
                  H.td $ Blaze.toHtml (show $ observedFailureAt event)
                  H.td $ Blaze.toHtml (failedEventType event)
                  H.td $ if Sequence.null (failureReasons event)
                    then "Nothing logged"
                    else H.pre $
                      H.ul (mapM_ (H.li . H.toMarkup) $ failureReasons event)
                  H.td $ H.toMarkup $ failedEventBody event

            H.input ! A.type_ "submit" ! A.value "Retry Selected Events"

blaze :: Snap.MonadSnap m => Blaze.Html -> m ()
blaze response = do
  Snap.modifyResponse $ Snap.addHeader "Content-Type" "text/html; charset=UTF-8"
  Snap.writeLBS $ Blaze.renderHtml response

--------------------------------------------------------------------------------
-- | Retry all event IDs in a form submission, and then redirect back to /
retry :: Snap.Handler CaaAdmin CaaAdmin ()
retry = do
  eventIds <-
    map (read . Text.unpack . Text.decodeUtf8) . fromMaybe [] . Map.lookup "event_id"
      <$> Snap.getPostParams

  retryEvent <- Snap.getsSnapletState (view $ Snap.snapletValue . to retryMessage)
  Snap.with acid (SnapletAcidState.update $ DeleteEvents eventIds)
    >>= liftIO . mapM_ retryEvent

--------------------------------------------------------------------------------
-- | Listen on the failed events queue of the Cover Art Archive, and re-route
-- any messages into the Acid State store.
consumeFailures :: AMQP.Connection -> Text -> AcidState.AcidState FailedEvents -> IO (IO ())
consumeFailures conn failureQueue allFailures = do
  rabbitChan <- AMQP.openChannel conn
  retryChan <- AMQP.openChannel conn

  consumer <- AMQP.consumeMsgs rabbitChan failureQueue AMQP.Ack $
    \(msg, env) -> do
      result <- runMaybeT $ do
        evType <- MaybeT $ return $ case AMQP.envRoutingKey env of
          "index" -> Just Index
          "delete" -> Just Delete
          "move" -> Just Move
          _ -> Nothing

        reasons <- MaybeT $ return $ fmap Sequence.fromList $
          case AMQP.msgHeaders msg of
            Just (AMQP.FieldTable m) ->
              case Map.lookup "mb-exceptions" m of
                Just (AMQP.FVFieldArray reasons) ->
                  traverse ?? reasons $
                    \r -> case r of
                      AMQP.FVString s -> Just s
                      _ -> Nothing

                _ -> Just []
            _ -> Just []

        now <- liftIO getCurrentTime

        liftIO $
          AcidState.update allFailures $
            AppendFailedEvent
              reasons now evType
              (Text.decodeUtf8 $ AMQP.msgBody msg ^. strict)

      case result of
        Nothing -> requeue retryChan env
        Just _ -> AMQP.ackEnv env

  return $ AMQP.cancelConsumer rabbitChan consumer

 where

  requeue chan env = AMQP.rejectMsg chan (AMQP.envDeliveryTag env) True

--------------------------------------------------------------------------------
main :: IO ()
main = Snap.serveSnaplet Snap.defaultConfig initCaaAdmin
