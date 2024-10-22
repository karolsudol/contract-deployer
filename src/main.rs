use ethers::{
    providers::{Http, Middleware, Provider},
    types::{Address, BlockNumber, H256, U256},
};
use std::str::FromStr;
use eyre::Result;
use hex;

#[tokio::main]
async fn main() -> Result<()> {
    let provider = Provider::<Http>::try_from("https://bartio.rpc.berachain.com")?;
    
    let contract_address = Address::from_str("0x11B9e4B15d3E8bB460CAD6D45ad9AeD8af761edC")?;

    // Known block number
    let known_block_number = 3992892;

    // AccountCreation event signature
    let account_creation_signature = H256::from_str("0x8967dcaa00d8fcb9bb2b5beff4aaf8c020063512cf08fbe11fec37a1e3a150f2")?;

    if let Ok(Some(block)) = provider.get_block_with_txs(BlockNumber::Number(known_block_number.into())).await {
        for tx in block.transactions {
            if let Ok(Some(receipt)) = provider.get_transaction_receipt(tx.hash).await {
                for log in receipt.logs {
                    if log.topics.get(0) == Some(&account_creation_signature) &&
                       log.topics.get(1) == Some(&H256::from(contract_address)) {
                        println!("Contract Creator: {:?}", tx.from);
                        println!("Deployment transaction hash: {:?}", tx.hash);
                        println!("Block number: {}", known_block_number);
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
    
    println!("Could not find AccountCreation event in the specified block");

    Ok(())
}
