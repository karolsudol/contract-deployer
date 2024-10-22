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
use csv::{WriterBuilder, ReaderBuilder};
use std::fs::OpenOptions;

#[tokio::main]
async fn main() -> Result<()> {
    let provider = Arc::new(Provider::<Http>::try_from("https://bartio.rpc.berachain.com")?);
    
    // Read contract addresses from input CSV
    let mut contracts = Vec::new();
    let mut user_ops_counts = Vec::new();
    
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_path("contracts.csv")?;
        
    for result in rdr.records() {
        let record = result?;
        contracts.push(Address::from_str(&record[0].to_lowercase())?);
        user_ops_counts.push(record[1].to_string());
    }

    let start_block = 3992892 - 2;
    let end_block = 3992892 + 2;

    // AccountCreation event signature
    let account_creation_signature = H256::from_str("0x8967dcaa00d8fcb9bb2b5beff4aaf8c020063512cf08fbe11fec37a1e3a150f2")?;

    println!("Scanning blocks {} to {} for {} contracts", start_block, end_block, contracts.len());

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

    for (i, contract_address) in contracts.iter().enumerate() {
        let found_event = Arc::new(Mutex::new(false));
        println!("Processing contract {} of {}: {:?}", i + 1, contracts.len(), contract_address);

        let _results = stream::iter(start_block..=end_block)
            .map(|block_number| {
                let provider = Arc::clone(&provider);
                let contract_address = *contract_address;
                let account_creation_signature = account_creation_signature;
                let found_event = Arc::clone(&found_event);
                let csv_writer = Arc::clone(&csv_writer);
                let user_ops_count = user_ops_counts[i].clone();

                async move {
                    if let Ok(Some(block)) = provider.get_block_with_txs(BlockNumber::Number(block_number.into())).await {
                        for tx in block.transactions {
                            if let Ok(Some(receipt)) = provider.get_transaction_receipt(tx.hash).await {
                                for log in receipt.logs.iter() {
                                    if log.topics.get(0) == Some(&account_creation_signature) &&
                                       log.topics.get(1) == Some(&H256::from(contract_address)) {
                                        println!("Found creator for contract {:?}: {:?}", contract_address, tx.from);
                                        
                                        // Write to CSV
                                        let mut writer = csv_writer.lock().await;
                                        writer.write_record(&[
                                            contract_address.to_string(),
                                            user_ops_count,
                                            tx.from.to_string(),
                                        ])?;
                                        writer.flush()?;

                                        *found_event.lock().await = true;
                                        return Ok(()) as Result<(), eyre::Report>;
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

        if !*found_event.lock().await {
            println!("Could not find creator for contract {:?}", contract_address);
            // Write to CSV with n/a for EOA
            let mut writer = csv_writer.lock().await;
            writer.write_record(&[
                contract_address.to_string(),
                user_ops_counts[i].clone(),
                "n/a".to_string(),
            ])?;
            writer.flush()?;
        }
    }

    Ok(())
}