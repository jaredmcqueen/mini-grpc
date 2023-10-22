use std::sync::atomic::AtomicBool;

use mini_alpaca::Endpoint;
use mini_alpaca::StockClient;
use sherpa::sherpa_streamer_server::SherpaStreamer;
use sherpa::sherpa_streamer_server::SherpaStreamerServer;
// use stock::StockRequest;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};

use stock::StockData;
use stock::StockRequest;

use crate::stock::Trade;
use crate::stock::UpdatedBar;

pub mod sherpa {
    tonic::include_proto!("sherpa");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("sherpa_descriptor");
}

pub mod stock {
    tonic::include_proto!("stock");
}

pub struct MyStockStreamer {
    // channel: Arc<Receiver<mini_alpaca::stock::Event>>,
    sender: broadcast::Sender<mini_alpaca::stock::Event>,
}

#[tonic::async_trait]
impl SherpaStreamer for MyStockStreamer {
    type GetStockStream = ReceiverStream<Result<stock::StockData, Status>>;

    async fn get_stock(
        &self,
        request: Request<StockRequest>, // Accept request of type HelloRequest
    ) -> Result<Response<Self::GetStockStream>, Status> {
        println!("Got a request from {:?}", request.remote_addr());
        let inner = request.into_inner();
        let (tx, rx) = mpsc::channel(10_000);
        let mut message_rx = self.sender.subscribe();
        tokio::spawn(async move {
            loop {
                match message_rx.recv().await {
                    Ok(message) => match message {
                        mini_alpaca::stock::Event::Trade(event) => {
                            if inner.trade {
                                let trade = Trade {
                                    id: event.id,
                                    symbol: event.symbol,
                                    price: event.price,
                                    exchange_code: event.exchange_code,
                                    size: event.size,
                                    timestamp: event.timestamp,
                                    condition: event.condition,
                                    tape: event.tape,
                                };
                                let data = Some(stock::stock_data::Data::Trade(trade));
                                if let Err(e) = tx.send(Ok(StockData { data })).await {
                                    println!("error sending to client, {}", e);
                                    break;
                                }
                            }
                        }
                        mini_alpaca::stock::Event::TradeCorrection(event) => {
                            if inner.trade_correction {
                                let trade_correction = crate::stock::TradeCorrection {
                                    symbol: event.symbol,
                                    exchange_code: event.exchange_code,
                                    original_id: event.original_id,
                                    original_price: event.original_price,
                                    original_size: event.original_size,
                                    original_conditions: event.original_conditions,
                                    corrected_id: event.corrected_id,
                                    corrected_price: event.corrected_price,
                                    corrected_size: event.corrected_size,
                                    corrected_conditions: event.corrected_conditions,
                                    timestamp: event.timestamp,
                                    tape: event.tape,
                                };
                                let data = Some(stock::stock_data::Data::TradeCorrection(
                                    trade_correction,
                                ));
                                if let Err(e) = tx.send(Ok(StockData { data })).await {
                                    println!("error sending to client, {}", e);
                                    break;
                                }
                            }
                        }
                        mini_alpaca::stock::Event::TradeCancel(event) => {
                            if inner.trade_cancel {
                                let trade_cancel = crate::stock::TradeCancel {
                                    symbol: event.symbol,
                                    id: event.id,
                                    exchange_code: event.exchange_code,
                                    price: event.price,
                                    size: event.size,
                                    action: event.action,
                                    timestamp: event.timestamp,
                                    tape: event.tape,
                                };
                                let data = Some(stock::stock_data::Data::TradeCancel(trade_cancel));
                                if let Err(e) = tx.send(Ok(StockData { data })).await {
                                    println!("error sending to client, {}", e);
                                    break;
                                }
                            }
                        }
                        mini_alpaca::stock::Event::Quote(event) => {
                            if inner.quote {
                                let quote = crate::stock::Quote {
                                    symbol: event.symbol,
                                    ask_exchange_code: event.ask_exchange_code,
                                    ask_price: event.ask_price,
                                    ask_size: event.ask_size,
                                    bid_exchange_code: event.bid_exchange_code,
                                    bid_price: event.bid_price,
                                    bid_size: event.bid_size,
                                    condition: event.condition,
                                    timestamp: event.timestamp,
                                    tape: event.tape,
                                };
                                let data = Some(stock::stock_data::Data::Quote(quote));
                                if let Err(e) = tx.send(Ok(StockData { data })).await {
                                    println!("error sending to client, {}", e);
                                    break;
                                }
                            }
                        }
                        mini_alpaca::stock::Event::Bar(event) => {
                            if inner.bar {
                                let bar = crate::stock::Bar {
                                    symbol: event.symbol,
                                    open: event.open,
                                    high: event.high,
                                    low: event.low,
                                    close: event.close,
                                    volume: event.volume,
                                    timestamp: event.timestamp,
                                };
                                let data = Some(stock::stock_data::Data::Bar(bar));
                                if let Err(e) = tx.send(Ok(StockData { data })).await {
                                    println!("error sending to client, {}", e);
                                    break;
                                }
                            }
                        }
                        mini_alpaca::stock::Event::DailyBar(event) => {
                            if inner.daily_bar {
                                let daily_bar = crate::stock::DailyBar {
                                    symbol: event.symbol,
                                    open: event.open,
                                    high: event.high,
                                    low: event.low,
                                    close: event.close,
                                    volume: event.volume,
                                    timestamp: event.timestamp,
                                };
                                let data = Some(stock::stock_data::Data::DailyBar(daily_bar));
                                if let Err(e) = tx.send(Ok(StockData { data })).await {
                                    println!("error sending to client, {}", e);
                                    break;
                                }
                            }
                        }
                        mini_alpaca::stock::Event::UpdatedBar(event) => {
                            if inner.updated_bar {
                                let updated_bar = UpdatedBar {
                                    symbol: event.symbol,
                                    open: event.open,
                                    high: event.high,
                                    low: event.low,
                                    close: event.close,
                                    volume: event.volume,
                                    timestamp: event.timestamp,
                                };
                                let data = Some(stock::stock_data::Data::UpdatedBar(updated_bar));
                                if let Err(e) = tx.send(Ok(StockData { data })).await {
                                    println!("error sending to client, {}", e);
                                    break;
                                }
                            }
                        }
                        mini_alpaca::stock::Event::Status(event) => {
                            if inner.status {
                                let status = crate::stock::Status {
                                    symbol: event.symbol,
                                    status_code: event.status_code,
                                    status_message: event.status_message,
                                    reason_code: event.reason_code,
                                    reason_message: event.reason_message,
                                    timestamp: event.timestamp,
                                    tape: event.tape,
                                };
                                let data = Some(stock::stock_data::Data::Status(status));
                                if let Err(e) = tx.send(Ok(StockData { data })).await {
                                    println!("error sending to client, {}", e);
                                    break;
                                }
                            }
                        }
                        mini_alpaca::stock::Event::Lulds(event) => {
                            if inner.luld {
                                let luld = crate::stock::Luld {
                                    symbol: event.symbol,
                                    limit_up: event.limit_up,
                                    limit_down: event.limit_down,
                                    indicator: event.indicator,
                                    timestamp: event.timestamp,
                                    tape: event.tape,
                                };
                                let data = Some(stock::stock_data::Data::Luld(luld));
                                if let Err(e) = tx.send(Ok(StockData { data })).await {
                                    println!("error sending to client, {}", e);
                                    break;
                                }
                            }
                        }
                        _ => {}
                    },
                    Err(e) => {
                        eprint!("problems bro {e}");
                        break;
                    }
                }
            }
            println!("end of loop");
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (sender, _receiver) = broadcast::channel::<mini_alpaca::stock::Event>(1_000);
    let run = AtomicBool::new(true);

    let sender2 = sender.clone();
    // start a thread that connects to alpaca and starts getting data
    tokio::spawn(async move {
        let handler = |event| {
            sender2.send(event).unwrap();
            Ok(())
        };
        // FIXME: unwrap fails
        let mut stock_client = StockClient::new(Endpoint::StocksProductionSip, handler)
            .await
            .unwrap();

        stock_client
            .subscribe(mini_alpaca::stock::Subscribe {
                // trades: Some(vec!["*".into()]),
                // quotes: Some(vec!["*".into()]),
                // bars: Some(vec!["*".into()]),
                // daily_bars: Some(vec!["*".into()]),
                // updated_bars: Some(vec!["*".into()]),
                statuses: Some(vec!["*".into()]),
                // lulds: Some(vec!["*".into()]),
                ..Default::default()
            })
            .await
            .unwrap();

        stock_client.event_loop(&run).await.unwrap();
    });

    let addr = "[::1]:10000".parse().unwrap();

    let reflector = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(sherpa::FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    let streamer = MyStockStreamer { sender };
    let svc = SherpaStreamerServer::new(streamer);

    println!("sherpa listening on: {}", addr);

    Server::builder()
        .add_service(svc)
        .add_service(reflector)
        .serve(addr)
        .await?;

    Ok(())
}
