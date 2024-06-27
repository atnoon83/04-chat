use anyhow::{anyhow, Result};
use dashmap::DashMap;
use futures::{stream::SplitStream, SinkExt, StreamExt};
use std::fmt::Formatter;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, mpsc::Sender},
};
use tokio_util::codec::{Framed, LinesCodec};
use tracing::level_filters::LevelFilter;
use tracing::{info, trace, warn};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{
    fmt,
    fmt::{format::FmtSpan, time::Uptime},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    Layer,
};

const MAX_SIZE: usize = 128;

#[derive(Debug)]
pub enum Message {
    UserJoin(String),
    UserLeft(String),
    Chat(String, String),
}

impl Message {
    pub fn user_join(username: impl Into<String>) -> Self {
        Message::UserJoin(username.into())
    }

    pub fn user_left(username: impl Into<String>) -> Self {
        Message::UserLeft(username.into())
    }

    pub fn chat(username: impl Into<String>, message: impl Into<String>) -> Self {
        Message::Chat(username.into(), message.into())
    }
}

impl std::fmt::Display for Message {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::UserJoin(username) => write!(f, "[SYSTEM] - {} joined the chat", username),
            Message::UserLeft(username) => write!(f, "[SYSTEM] - {} left the chat", username),
            Message::Chat(username, message) => write!(f, "[{}]: {}", username, message),
        }
    }
}

struct Peer {
    username: String,
    stream: SplitStream<Framed<TcpStream, LinesCodec>>,
}

impl Peer {
    pub fn new(username: String, stream: SplitStream<Framed<TcpStream, LinesCodec>>) -> Self {
        Peer { username, stream }
    }
}

struct ChatRoom {
    peers: DashMap<SocketAddr, Sender<Arc<Message>>>,
}

impl ChatRoom {
    pub fn new() -> Self {
        ChatRoom {
            peers: DashMap::new(),
        }
    }

    async fn add_peer(
        &self,
        username: impl Into<String>,
        addr: SocketAddr,
        stream: Framed<TcpStream, LinesCodec>,
    ) -> Peer {
        let (tx, mut rx) = mpsc::channel(MAX_SIZE);

        self.peers.insert(addr, tx);

        let (mut stream_sender, stream_receiver) = stream.split();
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                if let Err(e) = stream_sender.send(message.to_string()).await {
                    warn!("Failed to send message to {}: {:?}", addr, e);
                    break;
                }
            }
        });

        Peer::new(username.into(), stream_receiver)
    }

    async fn broadcast(&self, addr: SocketAddr, message: Arc<Message>) {
        for peer in self.peers.iter() {
            if peer.key() == &addr {
                continue;
            }
            if let Err(e) = peer.value().send(message.clone()).await {
                warn!("Failed to send message to {}: {:?}", peer.key(), e);
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _guard = init_tracing().await;

    let addr = "0.0.0.0:9527";
    let listener = TcpListener::bind(addr).await?;
    info!("Listening on: {}", addr);

    let chat_room = Arc::new(ChatRoom::new());

    loop {
        let (stream, client_addr) = listener.accept().await?;
        let chat_room_clone = chat_room.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(chat_room_clone, client_addr, stream).await {
                warn!("Error: {:?}", e);
            }
        });
    }
}

/// Initialize tracing with console and file logging
async fn init_tracing() -> WorkerGuard {
    let console = fmt::Layer::new()
        .with_span_events(FmtSpan::CLOSE)
        .with_timer(Uptime::default())
        .pretty()
        .with_filter(LevelFilter::TRACE);

    let file_appender = tracing_appender::rolling::never(".", "ecosystem.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
    let file = fmt::Layer::new()
        .with_writer(non_blocking)
        .with_ansi(false)
        // .pretty()
        .with_filter(LevelFilter::TRACE);

    tracing_subscriber::registry()
        .with(console)
        .with(file)
        .init();

    guard
}

async fn handle_connection(
    chat_room: Arc<ChatRoom>,
    addr: SocketAddr,
    stream: TcpStream,
) -> Result<()> {
    let mut framed = Framed::new(stream, LinesCodec::new());
    framed.send("Enter your username: ").await?;

    let username = match framed.next().await {
        Some(Ok(username)) => {
            if username.is_empty() {
                return Err(anyhow!("Username cannot be empty"));
            }
            username
        }
        Some(Err(e)) => {
            warn!("Client Connect Error: {:?}", e);
            return Err(e.into());
        }
        None => {
            warn!("Connection closed");
            return Err(anyhow!("Connection closed"));
        }
    };

    let mut peer = chat_room.add_peer(username, addr, framed).await;
    let message = Message::user_join(&peer.username);
    trace!("{}", message);
    chat_room.broadcast(addr, Arc::new(message)).await;

    while let Some(line) = peer.stream.next().await {
        let line = match line {
            Ok(line) => line,
            Err(e) => {
                warn!("Error: {:?}", e);
                break;
            }
        };

        if line.is_empty() {
            continue;
        }

        if line.to_lowercase() == "exit" {
            chat_room.peers.remove(&addr);
            let message = Message::user_left(&peer.username);
            info!("{}", message);
            chat_room.broadcast(addr, Arc::new(message)).await;
            break;
        }

        let message = Arc::new(Message::chat(&peer.username, line));
        trace!("{}", message);
        chat_room.broadcast(addr, message).await;
    }
    Ok(())
}
