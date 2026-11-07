//! TCP server for Kafka wire protocol handling incoming client connections.

use bytes::Bytes;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

use crate::backend::IggyBackend;
use crate::protocol::process_frame;
use crate::state::BridgeState;

pub struct BridgeServer<B: IggyBackend + 'static> {
    pub state: Arc<BridgeState<B>>,
    pub bind_address: String,
}

impl<B: IggyBackend + 'static> BridgeServer<B> {
    pub fn new(state: Arc<BridgeState<B>>, bind_address: String) -> Self {
        Self { state, bind_address }
    }

    /// Runs the TCP listener loop. Returns when canceled or if binding fails.
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let listener = TcpListener::bind(&self.bind_address).await?;
        info!(
            "Kafka bridge listening on {} (advertised as {}:{})",
            self.bind_address, self.state.advertised_host, self.state.advertised_port
        );

        loop {
            let (socket, remote_addr) = listener.accept().await?;
            debug!("Accepted connection from {}", remote_addr);
            let state = Arc::clone(&self.state);

            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, state).await {
                    debug!("Connection closed from {}: {}", remote_addr, e);
                }
            });
        }
    }
}

async fn handle_connection<B: IggyBackend + 'static>(
    mut stream: TcpStream,
    state: Arc<BridgeState<B>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut len_buf = [0u8; 4];

    loop {
        match stream.read_exact(&mut len_buf).await {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }

        let frame_len = i32::from_be_bytes(len_buf);
        if frame_len <= 0 || frame_len > 100 * 1024 * 1024 {
            return Err(format!("Invalid frame length: {frame_len}").into());
        }

        let mut body = vec![0u8; frame_len as usize];
        stream.read_exact(&mut body).await?;

        let response = match process_frame(Bytes::from(body), &state).await {
            Ok(resp) => resp,
            Err(e) => {
                warn!("Error processing Kafka frame: {e}");
                break;
            }
        };

        stream.write_all(&response).await?;
        stream.flush().await?;
    }

    Ok(())
}
