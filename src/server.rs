use async_std::{
    prelude::*,
    // task,
    // net::{TcpListener, ToSocketAddrs, TcpStream},
    // io::BufReader,
};
use tokio::{
    io::{BufReader, AsyncBufReadExt, AsyncWriteExt}, 
    net::{TcpListener, TcpStream, ToSocketAddrs, 
        tcp::OwnedWriteHalf
    }, 
    signal,
    task::JoinHandle,
    // sync::mpsc
};
use crossbeam::channel::{unbounded};
// use futures::channel::mpsc;
// use futures::sink::SinkExt;
use futures::{FutureExt};
// use std::sync::Mutex;
use std::collections::hash_map::{Entry, HashMap};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Sender<T> = crossbeam::channel::Sender<T>;
type Receiver<T> = crossbeam::channel::Receiver<T>;

#[derive(Debug)]
enum Void {}

#[derive(Debug)]
enum Event {
    NewPeer {
        name: String,
        writer: OwnedWriteHalf,
        shutdown: Receiver<Void>
    },
    Message {
        from: String,
        to: Vec<String>,
        msg: String
    }
}

#[tokio::main]
pub(crate) async fn main() -> Result<()> {
    accept_loop("127.0.0.1:8080").await
}

async fn accept_loop(addr: impl ToSocketAddrs) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;

    let (broker_sender, broker_receiver) = unbounded();
    let broker_handle = tokio::spawn(broker_loop(broker_receiver));
        
    loop {
        futures::select! {
            _ = signal::ctrl_c().fuse() => {
                println!("Sever shutdown!");
                break
            },
            conn = listener.accept().fuse() => match conn {
                Ok((stream, client)) => {
                    println!("Accepting from:{}", client);
                    spawn_and_log_error(connection_loop(broker_sender.clone(), stream));
                },
                Err(e) => println!("{}", e)
            }
        }
    }

    drop(broker_sender);
    broker_handle.await?;

    Ok(())
}

async fn connection_loop(broker: Sender<Event>, stream: TcpStream) -> Result<()> {
    let (reader, writer) = stream.into_split();
    let mut lines_from_client = BufReader::new(reader).lines();

    let name = match lines_from_client.next_line().await? {
        Some(line) => line,
        None => Err("peer disconnected immediately")?
    };

    let (_shutdown_sender, shutdown_receiver) = unbounded::<Void>();
    broker.send(Event::NewPeer { 
        name: name.clone(), 
        writer,
        shutdown: shutdown_receiver
    });

    while let Some(line) = lines_from_client.next_line().await? {
        let line = line;
        let (dest, msg) = match line.find(':') {
            Some(idx) => (&line[..idx], line[idx + 1 ..].trim()),
            None => continue
        };
        let dest: Vec<String> = dest.split(',').map(|name| name.trim().to_string()).collect();
        let msg: String = msg.trim().to_string();

        broker.send(Event::Message {
            from: name.clone(),
            to: dest,
            msg,
        });
    }

    Ok(())
}

async fn connection_writer_loop(
    messages: &mut Receiver<String>, 
    mut writer: OwnedWriteHalf,
    shutdown: Receiver<Void>
) -> Result<()> {
    loop {
        crossbeam::select! {
            recv(messages) -> msg => match msg {
                Ok(msg) => {
                    writer.write_all(msg.as_bytes()).await?
                },
                Err(_) => break
            },
            recv(shutdown) -> void => match void {
                Ok(void) => match void {},
                Err(_) => break
            }
        }
    }

    Ok(())
}

async fn broker_loop(events: Receiver<Event>) {
    let (disconnect_sender, mut disconnect_receiver) = unbounded::<(String, Receiver<String>)>();
    let mut peers: HashMap<String, Sender<String>> = HashMap::new();

    loop {
        let event = crossbeam::select! {
            recv(events) -> event => match event {
                Ok(event) => event,
                Err(_) => break
            },
            recv(disconnect_receiver) -> disconnect => {
                let (name, _pending_messages) = disconnect.unwrap();
                assert!(peers.remove(&name).is_some());
                continue;
            }
        };

        match event {
            Event::Message { from, to, msg } => {
                for addr in to {
                    if let Some(peer) = peers.get_mut(&addr) {
                        let msg = format!("from {}: {}\n", from, msg);
                        peer.send(msg);
                    }
                }
            }
            Event::NewPeer { name, writer, shutdown } => {
                match peers.entry(name.clone()) {
                    Entry::Occupied(..) => (),
                    Entry::Vacant(entry) => {
                        let (client_sender, mut client_receiver) = unbounded();
                        entry.insert(client_sender);
                        let mut disconnect_sender = disconnect_sender.clone();
                        
                        spawn_and_log_error(async move {
                            let res = connection_writer_loop(&mut client_receiver, writer, shutdown).await;
                            disconnect_sender.send((name, client_receiver));
                            res
                        });
                    }
                }
            }
        }
    }
    drop(peers);
    drop(disconnect_sender);
    while let Some((_name, _pending_messages)) = disconnect_receiver.iter().next(){}
}

fn spawn_and_log_error<F>(fut: F) -> JoinHandle<()> 
    where F: Future<Output = Result<()>> + Send + 'static, 
{
    tokio::spawn(async move {
        if let Err(e) = fut.await {
            eprintln!("{}", e)
        }
    })    
}