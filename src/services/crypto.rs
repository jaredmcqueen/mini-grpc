use tokio::sync::broadcast::Sender;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::crypto;
use crate::crypto::crypto_data::Data;
use crate::crypto::Bar;
use crate::crypto::CryptoData;
use crate::crypto::CryptoRequest;
use crate::crypto::DailyBar;
use crate::crypto::OrderBook;
use crate::crypto::Quote;
use crate::crypto::Trade;
use crate::crypto::UpdatedBar;
use crate::sherpa::crypto_streamer_server::CryptoStreamer;

pub struct MyCryptoStreamer {
    pub sender: Sender<mini_alpaca::crypto::Event>,
}

#[tonic::async_trait]
impl CryptoStreamer for MyCryptoStreamer {
    type GetCryptoStream = ReceiverStream<Result<CryptoData, Status>>;

    async fn get_crypto(
        &self,
        request: Request<CryptoRequest>, // Accept request of type HelloRequest
    ) -> Result<Response<Self::GetCryptoStream>, Status> {
        println!(
            "Got a GetCrypto (get_crypto) from {:?}",
            request.remote_addr()
        );
        let inner = request.into_inner();
        let (tx, rx) = mpsc::channel(10_000);
        let mut message_rx = self.sender.subscribe();
        tokio::spawn(async move {
            loop {
                match message_rx.recv().await {
                    Ok(message) => match message {
                        mini_alpaca::crypto::Event::Trade(event) => {
                            if inner.trade {
                                let trade = Trade {
                                    symbol: event.symbol,
                                    price: event.price,
                                    size: event.size,
                                    timestamp: event.timestamp,
                                    id: event.id,
                                    taker_side: event.taker_side,
                                };
                                let data = Some(Data::Trade(trade));
                                if let Err(e) = tx.send(Ok(CryptoData { data })).await {
                                    println!("error sending to client, {}", e);
                                    break;
                                }
                            }
                        }
                        mini_alpaca::crypto::Event::OrderBook(event) => {
                            if inner.order_book {
                                let order_book = OrderBook {
                                    symbol: event.symbol,
                                    timestamp: event.timestamp,
                                    bid: event
                                        .bid
                                        .iter()
                                        .map(|b| crypto::Book {
                                            price: b.price,
                                            size: b.size,
                                        })
                                        .collect(),
                                    ask: event
                                        .ask
                                        .iter()
                                        .map(|b| crypto::Book {
                                            price: b.price,
                                            size: b.size,
                                        })
                                        .collect(),
                                    reset: event.reset.unwrap_or(false),
                                };
                                let data = Some(Data::OrderBook(order_book));
                                if let Err(e) = tx.send(Ok(CryptoData { data })).await {
                                    println!("error sending to client, {}", e);
                                    break;
                                }
                            }
                        }
                        mini_alpaca::crypto::Event::Quote(event) => {
                            if inner.quote {
                                let quote = Quote {
                                    symbol: event.symbol,
                                    bid_price: event.bid_price,
                                    bid_size: event.bid_size,
                                    ask_price: event.ask_price,
                                    ask_size: event.ask_size,
                                    timestamp: event.timestamp,
                                };
                                let data = Some(Data::Quote(quote));
                                if let Err(e) = tx.send(Ok(CryptoData { data })).await {
                                    println!("error sending to client, {}", e);
                                    break;
                                }
                            }
                        }
                        mini_alpaca::crypto::Event::Bar(event) => {
                            if inner.bar {
                                let bar = Bar {
                                    symbol: event.symbol,
                                    open: event.open,
                                    high: event.high,
                                    low: event.low,
                                    close: event.close,
                                    volume: event.volume,
                                    timestamp: event.timestamp,
                                    num_trades: event.num_trades,
                                    volume_weigth: event.volume_weight,
                                };
                                let data = Some(Data::Bar(bar));
                                if let Err(e) = tx.send(Ok(CryptoData { data })).await {
                                    println!("error sending to client, {}", e);
                                    break;
                                }
                            }
                        }
                        mini_alpaca::crypto::Event::DailyBar(event) => {
                            if inner.daily_bar {
                                let daily_bar = DailyBar {
                                    symbol: event.symbol,
                                    open: event.open,
                                    high: event.high,
                                    low: event.low,
                                    close: event.close,
                                    volume: event.volume,
                                    timestamp: event.timestamp,
                                    num_trades: event.num_trades,
                                    volume_weigth: event.volume_weight,
                                };
                                let data = Some(Data::DailyBar(daily_bar));
                                if let Err(e) = tx.send(Ok(CryptoData { data })).await {
                                    println!("error sending to client, {}", e);
                                    break;
                                }
                            }
                        }
                        mini_alpaca::crypto::Event::UpdatedBar(event) => {
                            if inner.updated_bar {
                                let updated_bar = UpdatedBar {
                                    symbol: event.symbol,
                                    open: event.open,
                                    high: event.high,
                                    low: event.low,
                                    close: event.close,
                                    volume: event.volume,
                                    timestamp: event.timestamp,
                                    num_trades: event.num_trades,
                                    volume_weigth: event.volume_weight,
                                };
                                let data = Some(Data::UpdatedBar(updated_bar));
                                if let Err(e) = tx.send(Ok(CryptoData { data })).await {
                                    println!("error sending  client, {}", e);
                                    break;
                                }
                            }
                        }
                        _ => {
                            println!("wat")
                        }
                    },
                    Err(e) => match e {
                        tokio::sync::broadcast::error::RecvError::Closed => break,
                        tokio::sync::broadcast::error::RecvError::Lagged(e) => {
                            eprint!("crypto thread lagged: {}", e);
                        }
                    },
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
