use chrono::{DateTime, Utc};
use reed_solomon_erasure::galois_8::ReedSolomon;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc, time::Duration,
};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::UdpSocket};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

// Constants for Reed-Solomon encoding
const DATA_SHRED_COUNT: usize = 8; // Number of data shreds
const PARITY_SHRED_COUNT: usize = 4; // Number of parity shreds
const TOTAL_SHRED_COUNT: usize = DATA_SHRED_COUNT + PARITY_SHRED_COUNT;

// Represents a block of data that will be split into shreds
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Block {
    block_id: u64,
    slot: u64,
    data: Vec<u8>,
}

// Shred represents a piece of data in the Solana blockchain
// In the real Solana implementation, this would contain actual transaction data
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Shred {
    shred_id: u64,   // Unique identifier for the shred
    block_id: u64,   // ID of the block this shred belongs to
    index: usize,    // Index of this shred within the block
    is_parity: bool, // Whether this is a parity or data shred
    slot: u64,       // The slot this shred belongs to
    data: Vec<u8>,   // Actual shred data
    timestamp: DateTime<Utc>,
}
// Message enum defines all possible message types that nodes can exchange
#[derive(Debug, Serialize, Deserialize)]
enum Message {
    NewShred(Shred),
    RequestShreds(Vec<u64>),
    KnownShreds(Vec<u64>),
    RequestBlockRecovery(u64), // Request recovery of a specific block
}

#[derive(Debug, Clone)]
struct BlockRecoveryStatus {
    data_shreds: HashMap<usize, Vec<u8>>,   // Index -> Data
    parity_shreds: HashMap<usize, Vec<u8>>, // Index -> Data
    total_received: usize,
}

pub struct UDPGossipNode {
    id: String,
    address: SocketAddr,
    // Thread-safe containers for shared state:
    peers: Arc<RwLock<HashMap<String, SocketAddr>>>, // Map of peer IDs to their addresses
    known_shreds: Arc<RwLock<HashSet<u64>>>,         // Set of shred IDs we've seen
    shred_store: Arc<RwLock<HashMap<u64, Shred>>>,   // Storage for actual shreds
    block_recovery: Arc<RwLock<HashMap<u64, BlockRecoveryStatus>>>, // block_id -> recovery status
}

impl UDPGossipNode {
    pub fn new(id: String, address: SocketAddr) -> Self {
        UDPGossipNode {
            id,
            address,
            peers: Arc::new(RwLock::new(HashMap::new())),
            known_shreds: Arc::new(RwLock::new(HashSet::new())),
            shred_store: Arc::new(RwLock::new(HashMap::new())),
            block_recovery: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Single UDP socket for both sending and receiving
        let socket = Arc::new(UdpSocket::bind(self.address).await?);
        println!("Node {} listening on UDP {}", self.id, self.address);

        // Clone for receive loop
        let recv_socket = Arc::clone(&socket);
        let node_clone = self.clone_arc();

        // Spawn receive loop
        tokio::spawn(async move {
            let mut buf = vec![0; 65535];  // UDP max packet size
            loop {
                match recv_socket.recv_from(&mut buf).await {
                    Ok((len, peer_addr)) => {
                        // Clone only the received data
                        let packet = buf[..len].to_vec();
                        let node = node_clone.clone();
                        let socket = Arc::clone(&recv_socket);
                        
                        // Handle packet in separate task
                        tokio::spawn(async move {
                            if let Ok(message) = bincode::deserialize::<Message>(&packet) {
                                if let Err(e) = Self::handle_message(message, peer_addr, socket, node).await {
                                    eprintln!("Error handling message from {}: {}", peer_addr, e);
                                }
                            }
                        });
                    }
                    Err(e) => eprintln!("UDP receive error: {}", e),
                }
            }
        });

        Ok(())
    }

    // Create a new Arc reference to this node
    // Necessary for sharing the node across async tasks
    fn clone_arc(&self) -> Arc<Self> {
        Arc::new(Self {
            id: self.id.clone(),
            address: self.address,
            peers: Arc::clone(&self.peers),
            known_shreds: Arc::clone(&self.known_shreds),
            shred_store: Arc::clone(&self.shred_store),
            block_recovery: Arc::clone(&self.block_recovery),
        })
    }

    async fn process_shred(&self, shred: Shred) -> Result<(), Box<dyn std::error::Error>> {
        let mut known_shreds = self.known_shreds.write().await;
        if !known_shreds.contains(&shred.shred_id) {
            known_shreds.insert(shred.shred_id);
            self.shred_store
                .write()
                .await
                .insert(shred.shred_id, shred.clone());

            // Update block recovery status
            let mut block_recovery = self.block_recovery.write().await;
            let status = block_recovery
                .entry(shred.block_id)
                .or_insert(BlockRecoveryStatus {
                    data_shreds: HashMap::new(),
                    parity_shreds: HashMap::new(),
                    total_received: 0,
                });

            if shred.is_parity {
                status.parity_shreds.insert(shred.index, shred.data.clone());
            } else {
                status.data_shreds.insert(shred.index, shred.data.clone());
            }
            status.total_received += 1;

            drop(block_recovery);
            drop(known_shreds);

            // Check if we can recover and attempt recovery if possible
            if self.can_recover(shred.block_id).await {
                match self.try_recover_block(shred.block_id).await {
                    Ok(Some(recovered_data)) => {
                        println!(
                            "Successfully recovered block {} ({} bytes)",
                            shred.block_id,
                            recovered_data.len()
                        );
                        // Handle recovered data here
                    }
                    Ok(None) => {
                        println!("Not enough shreds to recover block {}", shred.block_id);
                    }
                    Err(e) => {
                        println!("Error recovering block {}: {}", shred.block_id, e);
                    }
                }
            }

            // Broadcast to peers
            self.broadcast_shred(shred).await?;
        }
        Ok(())
    }

    // Let's also add a helper method to check if we can recover
    async fn can_recover(&self, block_id: u64) -> bool {
        let block_recovery = self.block_recovery.read().await;
        if let Some(status) = block_recovery.get(&block_id) {
            // We need at least DATA_SHRED_COUNT total shreds (combination of data and parity)
            // to be able to recover the original data
            status.total_received >= DATA_SHRED_COUNT
        } else {
            false
        }
    }
   
    async fn handle_message(
        message: Message,
        peer_addr: SocketAddr,
        socket: Arc<UdpSocket>,
        node: Arc<UDPGossipNode>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match message {
            Message::NewShred(shred) => {
                println!("Received shred {} from {}", shred.shred_id, peer_addr);
                node.process_shred(shred).await?;
            }
            Message::RequestShreds(shred_ids) => {
                let shred_store = node.shred_store.read().await;
                for id in shred_ids {
                    if let Some(shred) = shred_store.get(&id) {
                        let message = Message::NewShred(shred.clone());
                        let encoded = bincode::serialize(&message)?;
                        socket.send_to(&encoded, peer_addr).await?;
                    }
                }
            }
            Message::KnownShreds(shred_ids) => {
                let known = node.known_shreds.read().await;
                let missing: Vec<_> = shred_ids
                    .into_iter()
                    .filter(|id| !known.contains(id))
                    .collect();

                if !missing.is_empty() {
                    let message = Message::RequestShreds(missing);
                    let encoded = bincode::serialize(&message)?;
                    socket.send_to(&encoded, peer_addr).await?;
                }
            }
            Message::RequestBlockRecovery(block_id) => {
                println!("Received recovery request for block {} from {}", block_id, peer_addr);
            }
        }
        Ok(())
    }

    // Try to recover a block using available shreds
    async fn try_recover_block(
        &self,
        block_id: u64,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let block_recovery = self.block_recovery.read().await;
        let recovery_status = match block_recovery.get(&block_id) {
            Some(status) => status.clone(),
            None => return Ok(None),
        };
        drop(block_recovery);

        // Create reed-solomon instance
        let reed_solomon = ReedSolomon::new(DATA_SHRED_COUNT, PARITY_SHRED_COUNT)
            .map_err(|e| format!("Failed to create Reed-Solomon: {}", e))?;

        // Get the shard size (all shreds must be same size)
        let shard_size = recovery_status
            .data_shreds
            .values()
            .chain(recovery_status.parity_shreds.values())
            .map(|v| v.len())
            .next()
            .ok_or("No shreds available")?;

        // Prepare data buffer for recovery
        let mut shards = vec![vec![0u8; shard_size]; TOTAL_SHRED_COUNT];
        let mut present = [false; TOTAL_SHRED_COUNT];

        // Fill in known data shreds
        for (i, data) in recovery_status.data_shreds {
            shards[i] = data;
            present[i] = true;
        }

        // Fill in known parity shreds
        for (i, data) in recovery_status.parity_shreds {
            shards[i] = data;
            present[i] = true;
        }

        // Check if we have enough shreds to attempt recovery
        let available_shreds: usize = present.iter().filter(|&&x| x).count();
        if available_shreds < DATA_SHRED_COUNT {
            return Ok(None);
        }

        // Convert shards to tuples of (slice, present) for reconstruction
        let mut shard_present: Vec<(&mut [u8], bool)> = shards
            .iter_mut()
            .zip(present.iter())
            .map(|(shard, &present)| (shard.as_mut_slice(), present))
            .collect();

        // Attempt reconstruction
        match reed_solomon.reconstruct_data(&mut shard_present) {
            Ok(_) => {
                // Combine recovered data shreds
                let mut block_data = Vec::new();
                for i in 0..DATA_SHRED_COUNT {
                    block_data.extend_from_slice(&shards[i]);
                }

                // Trim padding zeros from the end
                while block_data.last() == Some(&0) {
                    block_data.pop();
                }

                Ok(Some(block_data))
            }
            Err(e) => {
                println!("Failed to recover block {}: {}", block_id, e);
                Ok(None)
            }
        }
    }

    // Modified broadcast_shred for UDP with retries and acknowledgment
    async fn broadcast_shred(&self, shred: Shred) -> Result<(), Box<dyn std::error::Error>> {
        let message = Message::NewShred(shred.clone());
        let encoded = bincode::serialize(&message)?;

        // Get peers
        let peers = self.peers.read().await;
        
        // Create socket for broadcasting
        let socket = UdpSocket::bind("0.0.0.0:0").await?;

        const MAX_RETRIES: u32 = 3;
        const RETRY_DELAY_MS: u64 = 100;

        // Broadcast to all peers with retry logic
        for (peer_id, peer_addr) in peers.iter() {
            let mut retry_count = 0;
            while retry_count < MAX_RETRIES {
                match socket.send_to(&encoded, peer_addr).await {
                    Ok(_) => {
                        println!("Sent shred {} to peer {}", shred.shred_id, peer_id);
                        break;
                    }
                    Err(e) => {
                        retry_count += 1;
                        eprintln!(
                            "Failed to send to peer {} (attempt {}/{}): {}", 
                            peer_id, retry_count, MAX_RETRIES, e
                        );
                        if retry_count < MAX_RETRIES {
                            tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                        }
                    }
                }
            }
        }

        Ok(())
    }


}

