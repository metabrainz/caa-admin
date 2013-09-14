{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}
module Main where

import Prelude hiding (mapM_)

import Control.Applicative
import Control.Lens hiding ((|>))
import Control.Monad (join)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Reader.Class (asks)
import Control.Monad.State.Class (get, modify, put)
import Control.Monad.Trans.Maybe (MaybeT(..))
import Data.Foldable (forM_, mapM_)
import Data.Maybe (fromMaybe)
import Data.Monoid (Monoid(..))
import Data.Text (Text)
import Data.Time (UTCTime, getCurrentTime)
import Data.Typeable (Typeable)
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
data EventType = Index | Delete | Move
  deriving (Show)

instance H.ToMarkup EventType where
  toMarkup = H.toMarkup . show

data FailedEvent = FailedEvent { failureReasons :: Sequence.Seq Text
                               , failedAt :: UTCTime
                               , failedEventType :: EventType
                               , failedEventBody :: Text
                               , failedEventId :: Int
                               }
  deriving (Show, Typeable)

data FailedEvents = FailedEvents (IntMap.IntMap FailedEvent) Int
  deriving (Typeable)

--------------------------------------------------------------------------------
allFailedEvents :: AcidState.Query FailedEvents [FailedEvent]
allFailedEvents = asks $ \(FailedEvents m _) -> IntMap.elems m

appendFailedEvent
  :: Sequence.Seq Text -> UTCTime -> EventType -> Text
  -> AcidState.Update FailedEvents ()
appendFailedEvent reasons time evType body =
  modify $ \(FailedEvents events i) ->
    let event = FailedEvent reasons time evType body newId
        newId = i + 1
    in FailedEvents (IntMap.insert newId event events) newId

popEvents :: [Int] -> AcidState.Update FailedEvents [FailedEvent]
popEvents ids = do
  FailedEvents m nextId <- get
  let (popped, rest) = IntMap.partitionWithKey (\i _ -> i`elem` ids) m
  IntMap.elems popped <$ put (FailedEvents rest nextId)

SafeCopy.deriveSafeCopy 0 'SafeCopy.base ''EventType
SafeCopy.deriveSafeCopy 0 'SafeCopy.base ''FailedEvent
SafeCopy.deriveSafeCopy 0 'SafeCopy.base ''FailedEvents
AcidState.makeAcidic ''FailedEvents ['allFailedEvents, 'appendFailedEvent, 'popEvents]

--------------------------------------------------------------------------------
data CaaAdmin = CaaAdmin { _acid :: Snap.Snaplet (SnapletAcidState.Acid FailedEvents)
                         , retryMessage :: FailedEvent -> IO ()
                         }
makeLenses ''CaaAdmin

instance SnapletAcidState.HasAcid CaaAdmin FailedEvents where
  getAcidStore = view (acid . Snap.snapletValue)

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
      join $ AMQP.openConnection <$> caaOption "host" "127.0.0.1"
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
    liftIO $ print retryExchange
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
                  H.td $ Blaze.toHtml (show $ failedAt event)
                  H.td $ Blaze.toHtml (failedEventType event)
                  H.td $ if Sequence.null (failureReasons event)
                    then "Nothing logged"
                    else H.ul (mapM_ (H.li . H.toMarkup) $ failureReasons event)
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
  Snap.with acid (SnapletAcidState.update $ PopEvents eventIds)
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

        liftIO $ do
          result <- AcidState.update allFailures $
            AppendFailedEvent
              reasons now evType
              (Text.decodeUtf8 $ AMQP.msgBody msg ^. strict)
          print result

      case result of
        Nothing -> requeue retryChan env
        Just _ -> AMQP.ackEnv env

  return $ AMQP.cancelConsumer rabbitChan consumer

 where

  requeue chan env = AMQP.rejectMsg chan (AMQP.envDeliveryTag env) True

--------------------------------------------------------------------------------
main :: IO ()
main = Snap.serveSnaplet Snap.defaultConfig initCaaAdmin
