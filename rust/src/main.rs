use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::net::TcpStream;
use std::pin::Pin;
use std::process::ExitCode;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tokio_tungstenite::tungstenite::handshake::server::create_response;
use tokio_tungstenite::{WebSocketStream, connect_async, tungstenite::Message};
use tracing::{error, info, warn};

type INTERVAL = i8;
type BoxDynError = Box<dyn Error>;
type WSSWrite = SplitSink<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>, Message>;
type ChannelHandler = for<'a> fn(
  text: &'a str,
  json: Value,
  producer: FutureProducer,
) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
type ChannelHandlerMap = HashMap<&'static str, ChannelHandler>;

const KRAKEN_WS_URL: &str = "wss://ws.kraken.com/v2";
const SYMBOLS: [&str; 1] = ["XRP/USD"];
const INTERVALS: [INTERVAL; 3] = [1, 15, 60];

fn get_brokers() -> Result<String, BoxDynError> {
  match env::var("KAFKA_BROKERS") {
    Ok(brokers) => Ok(brokers),
    Err(e) => {
      error!("Expecting KAFKA_BROKERS value. None found.");
      return Err(e.into());
    }
  }
}

fn create_producer(brokers: String) -> Result<FutureProducer, BoxDynError> {
  let producer = ClientConfig::new()
  .set("bootstrap.servers", brokers)
  .set("message.timeout.ms", "5000")
  .set("queue.buffering.max.messages", "100000")
  .set("queue.buffering.max.kbytes", "1048576")
  .create::<FutureProducer>()?;
  Ok(producer)
}

async fn ticker_subscribe(write: &mut WSSWrite) -> Result<(), BoxDynError> {
  let msg = serde_json::json!({
    "method": "subscribe",
    "params": {
      "channel": "ticker",
      "symbol": SYMBOLS,
      "event_trigger": "trades",
      "snapshot": true
    }
  });
  let json = serde_json::to_string(&msg)?;
  let _ = write.send(Message::Text(Into::into(json))).await?;
  Ok(())
}

async fn ohlc_subscribe(write: &mut WSSWrite, interval: INTERVAL) -> Result<(), BoxDynError> {
  let msg = serde_json::json!({
    "method": "subscribe",
    "params": {
      "channel": "ticker",
      "symbol": SYMBOLS,
      "interval": interval,
      "snapshot": false
    }
  });
  let json = serde_json::to_string(&msg)?;
  let _ = write.send(Message::Text(Into::into(json))).await?;
  Ok(())
}

macro_rules! channel_handler_async {
  ($func:expr) => {
    (|text: &str, json: Value, producer: FutureProducer| {
      (Box::pin($func(&text, json, producer)))
    }) as ChannelHandler
  };
}

async fn ch_ignore<'a>(_: & 'a str, _: Value, _: FutureProducer) {}

// async fn ch_just_log(text: &str, _: Value, _: FutureProducer) {
//   info!("Received message: {}", &text[..text.len().min(200)]);
// }

// async fn ch_ticker(text: &str, json: Value, producer: FutureProducer) {
  // let record = FutureRecord::to("dj.kraken.ticker")
  //   .payload(&text)
  //   .key("something");
  // let delivery_status = producer.send(record, Duration::from_secs(0)).await;
// }

// async fn ch_ohlc(_text: &str, json: Value, _: FutureProducer) {}

fn create_channel_handler_map() -> ChannelHandlerMap {
  let mut map: HashMap<&str, ChannelHandler> = HashMap::new();
  // map.insert("status", channel_handler_async!(ch_just_log));
  map.insert("heartbeat", channel_handler_async!(ch_ignore));
  // map.insert("ticker", channel_handler_async!(ch_ticker));
  // map.insert("ohlc", channel_handler_async!(ch_ohlc));
  map
}

async fn handle_message(
  text: &str,
  channel_handler_map: &ChannelHandlerMap,
  producer: &FutureProducer,
) -> Option<()> {
  info!("Received message: {}", &text[..text.len().min(200)]);
  let json = serde_json::from_str::<Value>(&text).ok()?;
  let channel = json.get("channel").and_then(|v| v.as_str()).unwrap_or("");
  let func = channel_handler_map.get(channel)?;
  func(text, json, producer.clone()).await;
  Some(())
}

async fn establish_kraken_connection(
  interval: INTERVAL,
  with_ticker: bool,
  producer: &FutureProducer,
  channel_handler_map: &ChannelHandlerMap,
) -> Result<(), BoxDynError> {
  let (wsclient, _response) = connect_async(KRAKEN_WS_URL).await?;
  let (mut write, mut read) = wsclient.split();
  if with_ticker {
    ticker_subscribe(&mut write).await?;
  }
  ohlc_subscribe(&mut write, interval).await?;
  while let Some(msg) = read.next().await {
    match msg {
      Ok(Message::Text(text)) => {
        handle_message(&text, channel_handler_map, producer).await;
      }
      Ok(Message::Close(_)) => {}
      Ok(_) => {}
      Err(e) => {
        error!("Websocket error {}", e)
      }
    }
  }
  Ok(())
}

async fn establish_kraken_connections(producer: FutureProducer) {
  let channel_handler_map = create_channel_handler_map();
  let _wsclients = INTERVALS.iter().enumerate().map(|(idx, interval)| {
    establish_kraken_connection(*interval, idx == 0, &producer, &channel_handler_map)
  });
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
  let brokers = get_brokers()?;
  let producer = create_producer(brokers)?;
  establish_kraken_connections(producer).await;
  Ok(())
}
