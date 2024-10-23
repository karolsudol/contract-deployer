use ethers::{
    providers::{Http, Middleware, Provider},
    types::{Address, BlockNumber, Filter, Log, H256}
};
use std::str::FromStr;
use eyre::Result;
use csv::{ReaderBuilder, WriterBuilder};
use std::fs::OpenOptions;
use tokio::time::Duration;


#[derive(Debug)]
struct ContractInfo {
    address: Address,
    user_ops_count: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let provider = Provider::<Http>::try_from("https://bartio.rpc.berachain.com")?;
    
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
        });
    }

    // Create CSV writer for output
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open("output.csv")?;
    let mut writer = WriterBuilder::new()
        .has_headers(true)
        .from_writer(file);

    writer.write_record(&["smart_account_address", "usr_ops_cnt", "eoa"])?;
    writer.flush()?;

    // Correct event signature for AccountCreation
    let event_signature = "AccountCreation(address,address,uint256)";
    let topic = ethers::core::utils::keccak256(event_signature.as_bytes());
    let topic_hash = H256::from_slice(&topic);

    let start_block = 3992891u64;
    let end_block = 5925327u64;
    let chunk_size = 1000u64; // Reduced chunk size to handle potential RPC limitations

    // Create filter for the factory contract address
    let factory_address = Address::from_str("0x000000a56Aaca3e9a4C479ea6b6CD0DbcB6634F5")?;
    let filter_base = Filter::new()
        .topic0(topic_hash)
        .address(factory_address);

    println!("Fetching logs from block {} to {}", start_block, end_block);

    let logs = fetch_logs_in_chunks(
        &provider,
        start_block,
        end_block,
        chunk_size,
        filter_base
    ).await?;

    println!("Found {} logs", logs.len());

    for log in &logs {
        if let Some(account_address) = log.topics.get(1).map(|h| Address::from(*h)) {
            if let Some(auth_module) = log.topics.get(2).map(|h| Address::from(*h)) {
                // Get transaction to find the creator
                if let Some(tx_hash) = log.transaction_hash {
                    if let Ok(Some(tx)) = provider.get_transaction(tx_hash).await {
                        println!("Found account creation - Account: {}, Auth Module: {}, Creator: {}", 
                            account_address, 
                            auth_module,
                            tx.from);

                        // Find the corresponding contract info
                        if let Some(contract) = contracts.iter().find(|c| c.address == account_address) {
                            writer.write_record(&[
                                format!("{:#x}", account_address),
                                contract.user_ops_count.clone(),
                                format!("{:#x}", tx.from),
                            ])?;
                            writer.flush()?;
                        }
                    }
                }
            }
        }
    }

    // Write remaining unfound contracts
    let found_addresses: std::collections::HashSet<_> = logs
        .iter()
        .filter_map(|log| log.topics.get(1).copied())
        .map(Address::from)
        .collect();

    for contract in contracts.iter() {
        if !found_addresses.contains(&contract.address) {
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

async fn fetch_logs_in_chunks(
    provider: &Provider<Http>,
    start_block: u64,
    end_block: u64,
    chunk_size: u64,
    filter_base: Filter,
) -> Result<Vec<Log>> {
    let mut all_logs = Vec::new();
    let mut current_block = start_block;
    let mut current_chunk_size = chunk_size;

    while current_block <= end_block {
        let chunk_end = (current_block + current_chunk_size - 1).min(end_block);
        
        println!("Fetching logs for blocks {} to {}", current_block, chunk_end);
        
        let filter = filter_base.clone()
            .from_block(BlockNumber::Number(current_block.into()))
            .to_block(BlockNumber::Number(chunk_end.into()));

        match provider.get_logs(&filter).await {
            Ok(logs) => {
                println!("Found {} logs in chunk", logs.len());
                all_logs.extend(logs);
                current_block = chunk_end + 1;
            },
            Err(e) => {
                eprintln!("Error fetching logs: {}", e);
                // If chunk is too large, reduce size and retry current chunk
                if current_chunk_size > 100 {
                    println!("Reducing chunk size and retrying...");
                    current_chunk_size /= 2;
                    continue;
                } else {
                    return Err(e.into());
                }
            }
        }

        // Add delay to avoid rate limiting
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Ok(all_logs)
}