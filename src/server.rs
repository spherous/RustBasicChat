use async_std::prelude::*;
use mpsc::unbounded_channel;
use tokio::{
    io::{BufReader, AsyncBufReadExt, AsyncWriteExt}, 
    sync::mpsc, task::JoinHandle,
    net::{TcpListener, TcpStream, ToSocketAddrs, 
        tcp::OwnedWriteHalf
    }, 
    select
};
use std::collections::hash_map::{Entry, HashMap};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Sender<T> = mpsc::UnboundedSender<T>;
type Receiver<T> = mpsc::UnboundedReceiver<T>;

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

    let (broker_sender, broker_receiver) = unbounded_channel();
    let broker_handle = tokio::spawn(broker_loop(broker_receiver));
    
    while let Ok((stream, addr)) = listener.accept().await {
        println!("Accepting from:{}", addr);
        spawn_and_log_error(connection_loop(broker_sender.clone(), stream));
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

    let (_shutdown_sender, shutdown_receiver) = mpsc::unbounded_channel::<Void>();
    broker.send(Event::NewPeer { 
        name: name.clone(), 
        writer,
        shutdown: shutdown_receiver
    })?;

    while let Some(line) = lines_from_client.next_line().await? {
        let (dest, msg) = match line.find(':') {
            Some(idx) => (&line[..idx], line[idx + 1 ..].trim()),
            None => continue
        };
        let dest: Vec<String> = dest
            .split(',')
            .map(|name| name.trim().to_string())
            .collect();
        let msg: String = msg.trim().to_string();

        broker.send(Event::Message {
            from: name.clone(),
            to: dest,
            msg,
        })?;
    }

    Ok(())
}

async fn connection_writer_loop(
    messages: &mut Receiver<String>, 
    mut writer: OwnedWriteHalf,
    shutdown: Receiver<Void>
) -> Result<()> {
    let mut shutdown = shutdown;

    loop {
        select! {
            msg = messages.recv() => match msg {
                Some(msg) => if let Err(e) = writer.write(msg.as_bytes()).await {
                    println!("Error when writing to stream: {:?}", e);
                }
                None => break
            },
            void = shutdown.recv() => match void {
                Some(void) => match void {},
                None => break
            }
        }
    }

    Ok(())
}

async fn broker_loop(events: Receiver<Event>) {
    let (disconnect_sender, mut disconnect_receiver) = 
        mpsc::unbounded_channel::<(String, Receiver<String>)>();
    let mut peers: HashMap<String, Sender<String>> = HashMap::new();
    let mut events = events;

    loop {
        let event = select! {
            event = events.recv() => match  event {
                Some(event) => event,
                None => break
            },
            disconnect = disconnect_receiver.recv() => {
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
                        if let Err(e) = peer.send(msg) {
                            println!("Couldn't send message: {:?}", e);
                        }
                    }
                }
            }
            Event::NewPeer { name, writer, shutdown } => {
                match peers.entry(name.clone()) {
                    Entry::Occupied(..) => (),
                    Entry::Vacant(entry) => {
                        let (client_sender, mut client_receiver) = mpsc::unbounded_channel();
                        entry.insert(client_sender);
                        let disconnect_sender = disconnect_sender.clone();
                        
                        spawn_and_log_error(async move {
                            let res = 
                                connection_writer_loop(&mut client_receiver, writer, shutdown).await;
                            if let Err(e) = disconnect_sender.send((name, client_receiver)) {
                                println!("Couldn't send disconnect: {:?}", e);
                            }
                            res
                        });
                    }
                }
            }
        }
    }
    drop(peers);
    drop(disconnect_sender);
    while let Some((_name, _pending_messages)) = disconnect_receiver.recv().await {}
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