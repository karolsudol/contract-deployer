use ethers::{
    providers::{Http, Middleware, Provider},
    types::{Address, H256, BlockNumber, U256},
};
use std::str::FromStr;
use eyre::Result;
use hex;
// use futures::stream::{self, StreamExt};

#[tokio::main]
async fn main() -> Result<()> {
    let provider = Provider::<Http>::try_from("https://bartio.rpc.berachain.com")?;
    
    let contract_address = Address::from_str("0x11B9e4B15d3E8bB460CAD6D45ad9AeD8af761edC")?;

    // Start 100 blocks before the known block
    let start_block = 3992892 - 2;
    let end_block = start_block + 2; // Scan 2 blocks total

    // AccountCreation event signature
    let account_creation_signature = H256::from_str("0x8967dcaa00d8fcb9bb2b5beff4aaf8c020063512cf08fbe11fec37a1e3a150f2")?;

    println!("Scanning blocks {} to {}", start_block, end_block);

    for block_number in start_block..=end_block {
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
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    println!("Could not find AccountCreation event in the scanned blocks");
    Ok(())
}
