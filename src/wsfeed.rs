//! Contains structure which provides futures::Stream to websocket-feed of Coinbase api

use async_trait::async_trait;
use futures::{future, Sink, Stream};
use futures_util::{sink::SinkExt, stream::TryStreamExt};
use hyper::Method;
use serde_json;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_tungstenite::{connect_async, tungstenite::Message as TMessage};
use url::Url;

use crate::{private::Private, structs::wsfeed::*, ASync, CBError, WSError};

pub struct WSFeed;

fn convert_msg(msg: TMessage) -> Message {
    match msg {
        TMessage::Text(str) => serde_json::from_str(&str).unwrap_or_else(|e| {
            Message::InternalError(CBError::Serde {
                error: e,
                data: str,
            })
        }),
        _ => unreachable!(), // filtered in stream
    }
}

impl WSFeed {
    // Constructor for simple subcription with product_ids and channels
    pub async fn connect(
        uri: &str,
        product_ids: &[&str],
        channels: &[ChannelType],
    ) -> Result<impl CBStream + CBSink, CBError> {
        let subscribe = Subscribe {
            _type: SubscribeCmd::Subscribe,
            product_ids: product_ids.into_iter().map(|x| x.to_string()).collect(),
            channel : ChannelType::Level2,
            auth: None,
        };

        Self::connect_with_sub(uri, subscribe).await
    }

    // Constructor for extended subcription via Subscribe structure
    pub async fn connect_with_sub(
        uri: &str,
        subscribe: Subscribe,
    ) -> Result<impl CBStream + CBSink, CBError> {
        let url = Url::parse(uri).unwrap();

        let stream = connect_async(url)
            .await
            .map_err(|e| CBError::Websocket(WSError::Connect(e)))?
            .0;
        log::debug!("WebSocket handshake has been successfully completed");

        let mut stream = stream
            .try_filter(|msg| future::ready(msg.is_text()))
            .map_ok(convert_msg)
            .sink_map_err(|e| CBError::Websocket(WSError::Send(e)))
            .map_err(|e| CBError::Websocket(WSError::Read(e)));

        let subscribe = serde_json::to_string(&subscribe).unwrap();
        stream.send(TMessage::Text(subscribe)).await?;
        log::debug!("subsription sent");

        Ok(stream)
    }

    // Constructor for simple subcription with product_ids and channels with auth
    pub async fn connect_with_auth(
        uri: &str,
        product_ids: &[&str],
        channels: &[ChannelType],
        key: &str,
        secret: &str,
    ) -> Result<impl CBStream + CBSink, CBError> {
        let auth = Auth {
            name: key.to_string(),
            privateKey: secret.to_string(),
        };

        let subscribe = Subscribe {
            _type: SubscribeCmd::Subscribe,
            product_ids: product_ids.into_iter().map(|x| x.to_string()).collect(),
            channel : ChannelType::Level2,
            auth: Some(auth),
        };

        Self::connect_with_sub(uri, subscribe).await
    }
}

impl<T> CBSink for T where T: Sink<TMessage, Error = CBError> + Unpin + Send {}

#[async_trait]
pub trait CBSink: Sink<TMessage, Error = CBError> + Unpin + Send {
    async fn subscribe(
        &mut self,
        product_ids: &[&str],
        channels: &[ChannelType],
        auth: Option<Auth>,
    ) -> Result<(), CBError> {
        let subscribe = Subscribe {
            _type: SubscribeCmd::Subscribe,
            product_ids: product_ids.into_iter().map(|x| x.to_string()).collect(),
            channel : ChannelType::Level2,
            auth,
        };
        let subscribe = serde_json::to_string(&subscribe).unwrap();
        self.send(TMessage::Text(subscribe)).await
    }
}

impl<T> CBStream for T where T: Stream<Item = Result<Message, CBError>> + Unpin + Send {}

#[async_trait]
pub trait CBStream: Stream<Item = Result<Message, CBError>> + Unpin + Send {}
