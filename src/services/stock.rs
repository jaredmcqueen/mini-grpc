use tokio::sync::broadcast::Sender;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::sherpa::stock_streamer_server::StockStreamer;
use crate::stock::stock_data::Data;
use crate::stock::Bar;
use crate::stock::DailyBar;
use crate::stock::Luld;
use crate::stock::Quote;
use crate::stock::Status as StockStatus;
use crate::stock::StockData;
use crate::stock::StockRequest;
use crate::stock::Trade;
use crate::stock::TradeCancel;
use crate::stock::TradeCorrection;
use crate::stock::UpdatedBar;

pub struct MyStockStreamer {
    pub sender: Sender<mini_alpaca::stock::Event>,
}

#[tonic::async_trait]
impl StockStreamer for MyStockStreamer {
    type GetStockStream = ReceiverStream<Result<StockData, Status>>;

    async fn get_stock(
        &self,
        request: Request<StockRequest>, // Accept request of type HelloRequest
    ) -> Result<Response<Self::GetStockStream>, Status> {
        println!(
            "Got a GetStock (get_stock) from {:?}",
            request.remote_addr()
        );
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
                                let data = Some(Data::Trade(trade));
                                if let Err(e) = tx.send(Ok(StockData { data })).await {
                                    println!("error sending to client, {}", e);
                                    break;
                                }
                            }
                        }
                        mini_alpaca::stock::Event::TradeCorrection(event) => {
                            if inner.trade_correction {
                                let trade_correction = TradeCorrection {
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
                                let data = Some(Data::TradeCorrection(trade_correction));
                                if let Err(e) = tx.send(Ok(StockData { data })).await {
                                    println!("error sending to client, {}", e);
                                    break;
                                }
                            }
                        }
                        mini_alpaca::stock::Event::TradeCancel(event) => {
                            if inner.trade_cancel {
                                let trade_cancel = TradeCancel {
                                    symbol: event.symbol,
                                    id: event.id,
                                    exchange_code: event.exchange_code,
                                    price: event.price,
                                    size: event.size,
                                    action: event.action,
                                    timestamp: event.timestamp,
                                    tape: event.tape,
                                };
                                let data = Some(Data::TradeCancel(trade_cancel));
                                if let Err(e) = tx.send(Ok(StockData { data })).await {
                                    println!("error sending to client, {}", e);
                                    break;
                                }
                            }
                        }
                        mini_alpaca::stock::Event::Quote(event) => {
                            if inner.quote {
                                let quote = Quote {
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
                                let data = Some(Data::Quote(quote));
                                if let Err(e) = tx.send(Ok(StockData { data })).await {
                                    println!("error sending to client, {}", e);
                                    break;
                                }
                            }
                        }
                        mini_alpaca::stock::Event::Bar(event) => {
                            if inner.bar {
                                let bar = Bar {
                                    symbol: event.symbol,
                                    open: event.open,
                                    high: event.high,
                                    low: event.low,
                                    close: event.close,
                                    volume: event.volume,
                                    timestamp: event.timestamp,
                                };
                                let data = Some(Data::Bar(bar));
                                if let Err(e) = tx.send(Ok(StockData { data })).await {
                                    println!("error sending to client, {}", e);
                                    break;
                                }
                            }
                        }
                        mini_alpaca::stock::Event::DailyBar(event) => {
                            if inner.daily_bar {
                                let daily_bar = DailyBar {
                                    symbol: event.symbol,
                                    open: event.open,
                                    high: event.high,
                                    low: event.low,
                                    close: event.close,
                                    volume: event.volume,
                                    timestamp: event.timestamp,
                                };
                                let data = Some(Data::DailyBar(daily_bar));
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
                                let data = Some(Data::UpdatedBar(updated_bar));
                                if let Err(e) = tx.send(Ok(StockData { data })).await {
                                    println!("error sending to client, {}", e);
                                    break;
                                }
                            }
                        }
                        mini_alpaca::stock::Event::Status(event) => {
                            if inner.status {
                                let status = StockStatus {
                                    symbol: event.symbol,
                                    status_code: event.status_code,
                                    status_message: event.status_message,
                                    reason_code: event.reason_code,
                                    reason_message: event.reason_message,
                                    timestamp: event.timestamp,
                                    tape: event.tape,
                                };
                                let data = Some(Data::Status(status));
                                if let Err(e) = tx.send(Ok(StockData { data })).await {
                                    println!("error sending to client, {}", e);
                                    break;
                                }
                            }
                        }
                        mini_alpaca::stock::Event::Lulds(event) => {
                            if inner.luld {
                                let luld = Luld {
                                    symbol: event.symbol,
                                    limit_up: event.limit_up,
                                    limit_down: event.limit_down,
                                    indicator: event.indicator,
                                    timestamp: event.timestamp,
                                    tape: event.tape,
                                };
                                let data = Some(Data::Luld(luld));
                                if let Err(e) = tx.send(Ok(StockData { data })).await {
                                    println!("error sending to client, {}", e);
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
                            eprint!("stock thread lagged: {}", e);
                        }
                    },
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
