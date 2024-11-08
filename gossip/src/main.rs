use chrono::{DateTime, Utc};
use reed_solomon_erasure::galois_8::ReedSolomon;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

mod udp;

use crate::udp::UDPGossipNode;

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

struct GossipNode {
    id: String,
    address: SocketAddr,
    // Thread-safe containers for shared state:
    peers: Arc<RwLock<HashMap<String, SocketAddr>>>, // Map of peer IDs to their addresses
    known_shreds: Arc<RwLock<HashSet<u64>>>,         // Set of shred IDs we've seen
    shred_store: Arc<RwLock<HashMap<u64, Shred>>>,   // Storage for actual shreds
    block_recovery: Arc<RwLock<HashMap<u64, BlockRecoveryStatus>>>, // block_id -> recovery status
}

impl GossipNode {
    fn new(id: String, address: SocketAddr) -> Self {
        GossipNode {
            id,
            address,
            peers: Arc::new(RwLock::new(HashMap::new())),
            known_shreds: Arc::new(RwLock::new(HashSet::new())),
            shred_store: Arc::new(RwLock::new(HashMap::new())),
            block_recovery: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(self.address).await?;
        println!("Node {} listening on {}", self.id, self.address);

        // Accept connections indefinitely
        loop {
            let (socket, peer_addr) = listener.accept().await?;
            let node_clone = self.clone_arc();

            // Spawn a new task for each connection
            tokio::spawn(async move {
                if let Err(e) = Self::handle_connection(socket, peer_addr, node_clone).await {
                    eprintln!("Error handling connection from {}: {}", peer_addr, e);
                }
            });
        }
        Ok(())
    }

    // Create shreds from a block using Reed-Solomon encoding
    async fn create_shreds(&self, block: Block) -> Result<Vec<Shred>, Box<dyn std::error::Error>> {
        let encoder =
            ReedSolomon::new(DATA_SHRED_COUNT, PARITY_SHRED_COUNT).map_err(|e| format!("{}", e))?;


        // Split block data into DATA_SHRED_COUNT equal parts
        let chunk_size = (block.data.len() + DATA_SHRED_COUNT - 1) / DATA_SHRED_COUNT;

        println!("chunk size: {}", chunk_size);

        let mut data_shards: Vec<Vec<u8>> =
            block.data.chunks(chunk_size).map(|c| c.to_vec()).collect();

        println!("data_shards: {:?}", data_shards);

        // Pad the last chunk if necessary
        while data_shards.len() < DATA_SHRED_COUNT {
            data_shards.push(vec![0; chunk_size]);
        }

        // Ensure all chunks are the same size
        let max_len = data_shards.iter().map(|v| v.len()).max().unwrap_or(0);

        for shard in &mut data_shards {
            shard.resize(max_len, 0);
        }

        // Create parity shards
        let mut parity_shards = vec![vec![0u8; max_len]; PARITY_SHRED_COUNT];

        println!("parity_shards: {:?}", parity_shards);

        // Prepare mutable references for the encoder
        let mut shard_refs: Vec<&mut [u8]> = data_shards
            .iter_mut()
            .chain(parity_shards.iter_mut())
            .map(|shard| shard.as_mut_slice())
            .collect();

        // Encode the data (this creates the parity shards)
        encoder
            .encode(&mut shard_refs)
            .map_err(|e| format!("Failed to encode: {}", e))?;

        let mut shreds = Vec::with_capacity(TOTAL_SHRED_COUNT);

        // Create data shreds
        for (i, data) in data_shards.into_iter().enumerate() {
            let shred = Shred {
                shred_id: (block.block_id << 32) | i as u64,
                block_id: block.block_id,
                index: i,
                is_parity: false,
                slot: block.slot,
                data,
                timestamp: Utc::now(),
            };
            shreds.push(shred);
        }

        // Create parity shreds
        for (i, data) in parity_shards.into_iter().enumerate() {
            let shred = Shred {
                shred_id: (block.block_id << 32) | (DATA_SHRED_COUNT + i) as u64,
                block_id: block.block_id,
                index: DATA_SHRED_COUNT + i,
                is_parity: true,
                slot: block.slot,
                data,
                timestamp: Utc::now(),
            };
            shreds.push(shred);
        }

        Ok(shreds)
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

    // Broadcast a shred to all known peers
    async fn broadcast_shred(&self, shred: Shred) -> Result<(), Box<dyn std::error::Error>> {
        let message = Message::NewShred(shred);
        let encoded = bincode::serialize(&message)?;

        // Get read lock on peers
        let peers = self.peers.read().await;
        // Send to each peer
        for (peer_id, peer_addr) in peers.iter() {
            if let Ok(mut stream) = TcpStream::connect(*peer_addr).await {
                if let Err(e) = stream.write_all(&encoded).await {
                    eprintln!("Failed to send to peer {}: {}", peer_id, e);
                }
            }
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

    // Handle an individual connection with a peer
    async fn handle_connection(
        mut stream: TcpStream,
        peer_addr: SocketAddr,
        node: Arc<GossipNode>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut buffer = vec![0u8; 1024];

        // Read messages in a loop
        loop {
            let n = stream.read(&mut buffer).await?;
            if n == 0 {
                break Ok(()); // Connection closed
            }

            // Deserialize and handle the message
            let message: Message = bincode::deserialize(&buffer[..n])?;
            match message {
                // Handle new shred
                Message::NewShred(shred) => {
                    node.process_shred(shred).await?;
                }
                // Handle request for specific shreds
                Message::RequestShreds(shred_ids) => {
                    let shred_store = node.shred_store.read().await;
                    let shreds: Vec<_> = shred_ids
                        .iter()
                        .filter_map(|id| shred_store.get(id))
                        .cloned()
                        .collect();

                    // Send requested shreds back
                    for shred in shreds {
                        let message = Message::NewShred(shred);
                        let encoded = bincode::serialize(&message)?;
                        stream.write_all(&encoded).await?;
                    }
                }
                // Handle information about peer's known shreds
                Message::KnownShreds(shred_ids) => {
                    let known = node.known_shreds.read().await;
                    // Find shreds we're missing
                    let missing: Vec<_> = shred_ids
                        .into_iter()
                        .filter(|id| !known.contains(id))
                        .collect();

                    // Request missing shreds
                    if !missing.is_empty() {
                        let message = Message::RequestShreds(missing);
                        let encoded = bincode::serialize(&message)?;
                        stream.write_all(&encoded).await?;
                    }
                }
                Message::RequestBlockRecovery(_) => todo!(),
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create two nodes
    let node1 = Arc::new(GossipNode::new(
        "node1".to_string(),
        "127.0.0.1:8000".parse()?,
    ));

    let node2 = Arc::new(GossipNode::new(
        "node2".to_string(),
        "127.0.0.1:8001".parse()?,
    ));

    // Start both nodes
    let node1_clone = Arc::clone(&node1);
    let node2_clone = Arc::clone(&node2);

    let node1_handle = tokio::spawn(async move {
        node1_clone.start().await.unwrap();
    });

    let node2_handle = tokio::spawn(async move {
        node2_clone.start().await.unwrap();
    });

    // Wait for nodes to start
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Create test block
    let test_data = b"Hello world".to_vec();
    let block = Block {
        block_id: 1,
        slot: 100,
        data: test_data.clone(),
    };

    println!("Creating shreds from test block...");

    // Create shreds from the test block
    let shreds = node1.create_shreds(block).await?;
    // println!("loggin shreds {:?}", shreds);

    println!("Created {} shreds", shreds.len());

    // Process most data shreds (but not all) on node2
    println!("Processing shreds on node2...");
    for shred in shreds.iter().take(DATA_SHRED_COUNT - 1) {
        node2.process_shred(shred.clone()).await?;
    }

    // Add one parity shred to enable recovery
    println!("Adding parity shred for recovery...");
    node2
        .process_shred(shreds[DATA_SHRED_COUNT].clone())
        .await?;

    // Attempt recovery
    println!("Attempting block recovery...");
    match node2.try_recover_block(1).await? {
        Some(recovered_data) => {
            println!("Successfully recovered data!");
            println!("Original data length: {}", test_data.len());
            println!("Recovered data length: {}", recovered_data.len());
            assert_eq!(recovered_data, test_data, "Recovered data matches original");
            println!("Recovery test passed!");
        }
        None => {
            println!("Failed to recover data");
        }
    }

    // Keep the program running for a while to observe the recovery
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    Ok(())
}

// Add tests module
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_shred_recovery() -> Result<(), Box<dyn std::error::Error>> {
        let node = GossipNode::new("test_node".to_string(), "127.0.0.1:0".parse().unwrap());

        // Create test data
        let test_data =
            b"This is test data that will be split into shreds and recovered using Reed-Solomon"
                .to_vec();
        let block = Block {
            block_id: 1,
            slot: 1,
            data: test_data.clone(),
        };

        // Create shreds
        let all_shreds = node.create_shreds(block).await?;
        assert_eq!(all_shreds.len(), TOTAL_SHRED_COUNT);

        // Process most data shreds (but not all) and one parity shred
        for shred in all_shreds.iter().take(DATA_SHRED_COUNT - 1) {
            node.process_shred(shred.clone()).await?;
        }
        // Add one parity shred
        node.process_shred(all_shreds[DATA_SHRED_COUNT].clone())
            .await?;

        // Try recovery
        if let Some(recovered) = node.try_recover_block(1).await? {
            assert_eq!(
                recovered, test_data,
                "Recovered data doesn't match original"
            );
            println!("Successfully recovered data!");
        } else {
            panic!("Failed to recover data");
        }

        Ok(())
    }
}
