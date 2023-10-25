mod services;

use mini_alpaca::CryptoClient;
use mini_alpaca::Endpoint;
use mini_alpaca::NewsClient;
use mini_alpaca::StockClient;
use services::MyStockStreamer;
use sherpa::stock_streamer_server::StockStreamerServer;
use std::sync::atomic::AtomicBool;
use tokio::sync::broadcast;
use tonic::transport::Server;

use crate::services::MyCryptoStreamer;
use crate::sherpa::crypto_streamer_server::CryptoStreamerServer;
use crate::sherpa::news_streamer_server::NewsStreamerServer;

pub mod sherpa {
    tonic::include_proto!("sherpa");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("sherpa_descriptor");
}

pub mod stock {
    tonic::include_proto!("stock");
}
pub mod crypto {
    tonic::include_proto!("crypto");
}
pub mod news {
    tonic::include_proto!("news");
}

pub type Result<T> = std::result::Result<T, Error>;
pub type Error = Box<dyn std::error::Error + Send + Sync>;

async fn stock_handler(sender: broadcast::Sender<mini_alpaca::stock::Event>) -> Result<()> {
    let handler = |event| {
        sender
            .send(event)
            .expect("error when sending event to channel receiver");
        Ok(())
    };
    let mut stock_client = StockClient::new(Endpoint::StocksProductionSip, handler).await?;

    stock_client
        .subscribe(mini_alpaca::stock::Subscribe {
            trades: Some(vec!["*".into()]),
            quotes: Some(vec!["*".into()]),
            bars: Some(vec!["*".into()]),
            daily_bars: Some(vec!["*".into()]),
            updated_bars: Some(vec!["*".into()]),
            statuses: Some(vec!["*".into()]),
            lulds: Some(vec!["*".into()]),
            // ..Default::default()
        })
        .await?;

    //FIXME: cannot toggle run from the outside
    let run = AtomicBool::new(true);
    println!("running stock event loop");
    stock_client.event_loop(&run).await?;
    println!("exiting stock event loop");
    Ok(())
}

async fn crypto_handler(sender: broadcast::Sender<mini_alpaca::crypto::Event>) -> Result<()> {
    let handler = |event| {
        sender
            .send(event)
            .expect("error when sending event to channel receiver");
        Ok(())
    };
    let mut crypto_client = CryptoClient::new(Endpoint::Crypto, handler).await?;

    crypto_client
        .subscribe(mini_alpaca::crypto::Subscribe {
            trades: Some(vec!["*".into()]),
            quotes: Some(vec!["*".into()]),
            bars: Some(vec!["*".into()]),
            updated_bars: Some(vec!["*".into()]),
            dailly_bars: Some(vec!["*".into()]),
            order_books: Some(vec!["*".into()]),
            // ..Default::default()
        })
        .await?;

    //FIXME: cannot toggle run from the outside
    let run = AtomicBool::new(true);
    println!("running crypto event loop");
    crypto_client.event_loop(&run).await?;
    Ok(())
}

async fn news_handler(sender: broadcast::Sender<mini_alpaca::news::Event>) -> Result<()> {
    let handler = |event| {
        sender
            .send(event)
            .expect("error when sending event to channel receiver");
        Ok(())
    };
    let mut news_client = NewsClient::new(Endpoint::News, handler).await?;

    news_client
        .subscribe(mini_alpaca::news::Subscribe {
            news: Some(vec!["*".into()]),
            // ..Default::default()
        })
        .await?;

    //FIXME: cannot toggle run from the outside
    let run = AtomicBool::new(true);
    println!("running news event loop");
    news_client.event_loop(&run).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // broadcast channels.  the receiving end needs to stay open and not thrown away with a _
    let (stock_sender, _stock) = broadcast::channel::<mini_alpaca::stock::Event>(10_000);
    let (crypto_sender, _crypto) = broadcast::channel::<mini_alpaca::crypto::Event>(10_000);
    let (news_sender, _news) = broadcast::channel::<mini_alpaca::news::Event>(10_000);

    // clones of the senders, which make receivers when you .subscribe() to them
    let stock_sender_clone = stock_sender.clone();
    let crypto_sender_clone = crypto_sender.clone();
    let news_sender_clone = news_sender.clone();

    // handlers
    tokio::spawn(async move {
        if let Err(e) = stock_handler(stock_sender_clone).await {
            eprintln!("Error in stock handler: {}", e);
        }
    });
    tokio::spawn(async move {
        if let Err(e) = crypto_handler(crypto_sender_clone).await {
            eprintln!("Error in crypto handler: {}", e);
        }
    });
    tokio::spawn(async move {
        if let Err(e) = news_handler(news_sender_clone).await {
            eprintln!("Error in news handler: {}", e);
        }
    });

    let reflector = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(sherpa::FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    // services
    let blah = MyStockStreamer {
        sender: stock_sender,
    };
    let stock_service = StockStreamerServer::new(blah);
    let crypto_service = CryptoStreamerServer::new(MyCryptoStreamer {
        sender: crypto_sender,
    });
    let news_service = NewsStreamerServer::new(services::MyNewsStreamer {
        sender: news_sender,
    });

    let addr = "0.0.0.0:10000".parse().unwrap();
    println!("sherpa listening on: {}", addr);

    Server::builder()
        .add_service(stock_service)
        .add_service(crypto_service)
        .add_service(news_service)
        .add_service(reflector)
        .serve(addr)
        .await?;
    Ok(())
}
