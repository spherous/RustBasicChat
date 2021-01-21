use tokio::{
    io::{BufReader, stdin, AsyncWriteExt, AsyncBufReadExt},
    net::{TcpStream, ToSocketAddrs}
};
use futures::{select, FutureExt};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main]
pub(crate) async fn main() -> Result<()> {
    try_run("127.0.0.1:8080").await
}

async fn try_run(addr: impl ToSocketAddrs) -> Result<()> {
    let mut stream = TcpStream::connect(addr).await?;
    let (reader, mut writer) = stream.split();
    
    let mut lines_from_server = BufReader::new(reader).lines();
    let mut lines_from_stdin = BufReader::new(stdin()).lines();
    
    loop {
        select! {
            line = lines_from_server.next_line().fuse() => match line.unwrap() {
                Some(line) => println!("{}", line),
                None => break
            },
            line = lines_from_stdin.next_line().fuse() => match line.unwrap() {
                Some(line) => {
                    writer.write_all(line.as_bytes()).await?;
                    writer.write_all(b"\n").await?;
                },
                None => break
            }
        }
    }
    Ok(())   
}