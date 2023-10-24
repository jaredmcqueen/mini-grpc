use tokio::sync::broadcast::Sender;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::news::News;
use crate::news::NewsRequest;
use crate::sherpa::news_streamer_server::NewsStreamer;

pub struct MyNewsStreamer {
    pub sender: Sender<mini_alpaca::news::Event>,
}

#[tonic::async_trait]
impl NewsStreamer for MyNewsStreamer {
    type GetNewsStream = ReceiverStream<Result<News, Status>>;

    async fn get_news(
        &self,
        request: Request<NewsRequest>, // Accept request of type HelloRequest
    ) -> Result<Response<Self::GetNewsStream>, Status> {
        println!("Got a GetNews (get_news) from {:?}", request.remote_addr());
        let inner = request.into_inner();
        let (tx, rx) = mpsc::channel(10_000);
        let mut message_rx = self.sender.subscribe();
        tokio::spawn(async move {
            loop {
                match message_rx.recv().await {
                    Ok(message) => match message {
                        mini_alpaca::news::Event::News(event) => {
                            if inner.news {
                                let news = News {
                                    id: event.id,
                                    headline: event.headline,
                                    summary: event.summary,
                                    author: event.author,
                                    created: event.created_at,
                                    updated: event.updated_at,
                                    url: event.url,
                                    content: event.content,
                                    symbols: event.symbols,
                                    source: event.source,
                                };
                                if let Err(e) = tx.send(Ok(news)).await {
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
                            eprint!("news thread lagged: {}", e);
                        }
                    },
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
