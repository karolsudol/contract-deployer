use ethers::{
    providers::{Http, Middleware, Provider},
    types::{Address, H256, BlockNumber},
};
use std::str::FromStr;
use eyre::Result;
use futures::stream::{self, StreamExt};
use tokio::sync::Mutex;
use std::sync::Arc;
use csv::{WriterBuilder, ReaderBuilder};
use std::fs::OpenOptions;
use std::collections::HashMap;

struct ContractInfo {
    address: Address,
    user_ops_count: String,
    found: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let provider = Arc::new(Provider::<Http>::try_from("https://bartio.rpc.berachain.com")?);
    
    // Read contract addresses from input CSV
    let mut contracts = Vec::new();
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path("contracts.csv")?;
        
    for result in rdr.records() {
        let record = result?;
        contracts.push(ContractInfo {
            address: Address::from_str(&record[0].to_lowercase())?,
            user_ops_count: record[1].to_string(),
            found: false,
        });
    }

    let contracts = Arc::new(Mutex::new(contracts));
    let start_block = 2515813;
    let end_block = 5925327;

    // AccountCreation event signature
    let account_creation_signature = H256::from_str("0x8967dcaa00d8fcb9bb2b5beff4aaf8c020063512cf08fbe11fec37a1e3a150f2")?;

    println!("Scanning blocks {} to {}", start_block, end_block);

    // Create CSV writer for output
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open("output.csv")?;
    let csv_writer = Arc::new(Mutex::new(WriterBuilder::new()
        .has_headers(true)
        .from_writer(file)));

    // Write CSV headers
    {
        let mut writer = csv_writer.lock().await;
        writer.write_record(&["smart_account_address", "usr_ops_cnt", "eoa"])?;
        writer.flush()?;
    }

    // Create results map to store found EOAs
    let results = Arc::new(Mutex::new(HashMap::new()));

    let _results = stream::iter(start_block..=end_block)
        .map(|block_number| {
            let provider = Arc::clone(&provider);
            let account_creation_signature = account_creation_signature;
            let contracts = Arc::clone(&contracts);
            let results = Arc::clone(&results);
            let csv_writer = Arc::clone(&csv_writer);

            async move {
                println!("Scanning block {}", block_number);
                if let Ok(Some(block)) = provider.get_block_with_txs(BlockNumber::Number(block_number.into())).await {
                    for tx in block.transactions {
                        if let Ok(Some(receipt)) = provider.get_transaction_receipt(tx.hash).await {
                            for log in receipt.logs.iter() {
                                if log.topics.get(0) == Some(&account_creation_signature) {
                                    let mut contracts_lock = contracts.lock().await;
                                    for contract in contracts_lock.iter_mut() {
                                        if !contract.found && log.topics.get(1) == Some(&H256::from(contract.address)) {
                                            println!("Found creator for contract {}: {}", 
                                                contract.address, 
                                                tx.from);
                                            
                                            // Write to CSV immediately
                                            let mut writer = csv_writer.lock().await;
                                            writer.write_record(&[
                                                format!("{:#x}", contract.address),
                                                contract.user_ops_count.clone(),
                                                format!("{:#x}", tx.from),
                                            ])?;
                                            writer.flush()?;
                                            
                                            contract.found = true;
                                            
                                            let mut results_lock = results.lock().await;
                                            results_lock.insert(contract.address, tx.from);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(()) as Result<(), eyre::Report>
            }
        })
        .buffer_unordered(10)
        .collect::<Vec<_>>()
        .await;

    // Write remaining unfound contracts to CSV
    let contracts_lock = contracts.lock().await;
    let mut writer = csv_writer.lock().await;
    for contract in contracts_lock.iter() {
        if !contract.found {
            println!("Could not find creator for contract {}", contract.address);
            writer.write_record(&[
                format!("{:#x}", contract.address),
                contract.user_ops_count.clone(),
                "n/a".to_string(),
            ])?;
            writer.flush()?;
        }
    }

    Ok(())
}