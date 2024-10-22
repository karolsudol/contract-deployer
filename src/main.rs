use ethers::{
    providers::{Http, Middleware, Provider},
    types::{Address, H256, BlockNumber, U256},
};
use std::str::FromStr;
use eyre::Result;
use hex;
use futures::stream::{self, StreamExt};
use tokio::sync::Mutex;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let provider = Arc::new(Provider::<Http>::try_from("https://bartio.rpc.berachain.com")?);
    
    let contract_address = Address::from_str("0x11B9e4B15d3E8bB460CAD6D45ad9AeD8af761edC")?;

    // Start 40 blocks before the known block
    let start_block = 3992892 - 2;
    let end_block = 3992892 +200;

    // AccountCreation event signature
    let account_creation_signature = H256::from_str("0x8967dcaa00d8fcb9bb2b5beff4aaf8c020063512cf08fbe11fec37a1e3a150f2")?;

    println!("Scanning blocks {} to {}", start_block, end_block);

    let found_event = Arc::new(Mutex::new(false));

    let _results = stream::iter(start_block..=end_block)
        .map(|block_number| {
            let provider = Arc::clone(&provider);
            let contract_address = contract_address;
            let account_creation_signature = account_creation_signature;
            let found_event = Arc::clone(&found_event);

            async move {
                println!("Scanning block {}", block_number);

                if let Ok(Some(block)) = provider.get_block_with_txs(BlockNumber::Number(block_number.into())).await {
                    for tx in block.transactions {
                        if let Ok(Some(receipt)) = provider.get_transaction_receipt(tx.hash).await {
                            for (log_index, log) in receipt.logs.iter().enumerate() {
                                if log.topics.get(0) == Some(&account_creation_signature) &&
                                   log.topics.get(1) == Some(&H256::from(contract_address)) {
                                    println!("Found matching event in block {} transaction {} log {}", block_number, tx.hash, log_index);
                                    println!("Contract Creator: {:?}", tx.from);
                                    println!("Deployment transaction hash: {:?}", tx.hash);
                                    println!("Block number: {}", block_number);
                                    if let Some(initial_auth_module) = log.topics.get(2) {
                                        println!("Initial Auth Module: 0x{}", hex::encode(&initial_auth_module.0[12..]));
                                    }
                                    if let Some(index) = log.topics.get(3) {
                                        println!("Index: {}", U256::from(index.as_bytes()));
                                    }
                                    *found_event.lock().await = true;
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        })
        .buffer_unordered(10) // Process up to 10 blocks concurrently
        .collect::<Vec<_>>()
        .await;

    if !*found_event.lock().await {
        println!("Could not find AccountCreation event in the scanned blocks");
    }

    Ok(())
}
