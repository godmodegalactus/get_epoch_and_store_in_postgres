use std::{str::FromStr, sync::Arc};

use anyhow::Result;
use iter_tools::Itertools;
use serde::Serialize;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    borsh0_10::try_from_slice_unchecked,
    compute_budget::{self, ComputeBudgetInstruction},
    signature::Signature,
};
use solana_transaction_status::{UiTransactionEncoding, UiConfirmedBlock};

use crate::postgres::Postgres;
mod postgres;

#[derive(Clone, Serialize)]
pub struct TransactionConfirmRecord {
    pub signature: String,
    pub confirmed_slot: u64,
    pub writable_accounts: String,
    pub readable_account: String,
    pub cu_requested: u64,
    pub cu_consumed: u64,
    pub prioritization_fees: u64,
}

async fn fetch_data(
    rpc_client: &RpcClient,
    signatures: Vec<String>,
) -> Vec<TransactionConfirmRecord> {
    let signatures = signatures
        .iter()
        .filter_map(|x| Signature::from_str(x).ok())
        .collect_vec();
    let jhs = signatures
        .iter()
        .map(|signature| rpc_client.get_transaction(&signature, UiTransactionEncoding::Base64))
        .collect_vec();
    let results = futures::future::join_all(jhs).await;
    let mut return_vec = vec![];
    for result in results {
        match result {
            Ok(res) => {
                let transaction = res.transaction.transaction.decode().unwrap();
                let message = transaction.message;
                let legacy_compute_budget: Option<(u32, Option<u64>)> =
                    message.instructions().iter().find_map(|i| {
                        if i.program_id(message.static_account_keys())
                            .eq(&compute_budget::id())
                        {
                            if let Ok(ComputeBudgetInstruction::RequestUnitsDeprecated {
                                units,
                                additional_fee,
                            }) = try_from_slice_unchecked(i.data.as_slice())
                            {
                                if additional_fee > 0 {
                                    return Some((
                                        units,
                                        Some(((units * 1000) / additional_fee) as u64),
                                    ));
                                } else {
                                    return Some((units, None));
                                }
                            }
                        }
                        None
                    });

                let legacy_cu_requested = legacy_compute_budget.map(|x| x.0);

                let cu_requested = message
                    .instructions()
                    .iter()
                    .find_map(|i| {
                        if i.program_id(message.static_account_keys())
                            .eq(&compute_budget::id())
                        {
                            if let Ok(ComputeBudgetInstruction::SetComputeUnitLimit(limit)) =
                                try_from_slice_unchecked(i.data.as_slice())
                            {
                                return Some(limit);
                            }
                        }
                        None
                    })
                    .or(legacy_cu_requested);
                let cu_requested = cu_requested.unwrap_or(200000) as u64;
                let cu_consumed = match res.transaction.meta.unwrap().compute_units_consumed {
                    solana_transaction_status::option_serializer::OptionSerializer::Some(x) => x,
                    solana_transaction_status::option_serializer::OptionSerializer::Skip => 0,
                    solana_transaction_status::option_serializer::OptionSerializer::None => 0,
                };
                let prioritization_fees = message
                    .instructions()
                    .iter()
                    .find_map(|i| {
                        if i.program_id(message.static_account_keys())
                            .eq(&compute_budget::id())
                        {
                            if let Ok(ComputeBudgetInstruction::SetComputeUnitPrice(price)) =
                                try_from_slice_unchecked(i.data.as_slice())
                            {
                                return Some(price);
                            }
                        }

                        None
                    })
                    .unwrap_or(0);

                let writable_accounts = message
                    .static_account_keys()
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| message.is_maybe_writable(*index))
                    .map(|x| x.1.to_string())
                    .collect_vec();

                let readable_account = message
                    .static_account_keys()
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| !message.is_maybe_writable(*index))
                    .map(|x| x.1.to_string())
                    .collect_vec();

                return_vec.push(TransactionConfirmRecord {
                    signature: res.transaction.transaction.decode().unwrap().signatures[0]
                        .to_string(),
                    confirmed_slot: res.slot,
                    writable_accounts: writable_accounts.join(","),
                    readable_account: readable_account.join(","),
                    cu_consumed,
                    cu_requested,
                    prioritization_fees,
                });
            }
            Err(e) => {
                log::error!("error {}", e);
                continue;
            }
        }
    }
    return_vec
}

// old code
// let file = "in.csv";
// let mut rdr = csv_async::AsyncReader::from_reader(File::open(file).await?);

// let mut records = rdr.records();

// let mut buffer = vec![];
// let _ = records.next().await; // ignore first line
// let (sender, reciever) = tokio::sync::mpsc::unbounded_channel::<Vec<TransactionConfirmRecord>>();

// tokio::spawn(async move {
//     let mut writer =
//         csv_async::AsyncSerializer::from_writer(File::create("out.csv").await.unwrap());
//     let mut reciever = reciever;
//     loop {
//         let data = reciever.recv().await;
//         match data {
//             Some(data) => {
//                 println!("got array of {}", data.len());
//                 for dat in data {
//                     writer.serialize(dat).await.unwrap();
//                 }
//                 writer.flush().await.unwrap();
//             }
//             None => break,
//         }
//     }
// });

// while let Some(record) = records.next().await {
//     let record = record?;
//     let signature = record.get(0).unwrap();
//     buffer.push(signature.to_string());
//     if buffer.len() > 32 {
//         let res = fetch_data(&rpc_client, buffer.clone()).await;
//         sender.send(res)?;
//         buffer.clear();
//     }
// }

#[derive(Clone, Serialize)]
pub struct BlockInfo {
    pub block_height: u64,
    pub nb_transactions: usize,
    pub block_rewards: i64,
}

#[tokio::main()]
async fn main() -> Result<()> {
    let rpc_url = "https://mango.rpcpool.com/7ea66877e68a97698f654b5e7a0f";
    let rpc_client = Arc::new(RpcClient::new(rpc_url.to_string()));
    let epoch_info = rpc_client.get_epoch_info().await?;
    let previous_epoch = epoch_info.epoch - 1;
    println!("Previous epoch : {}", previous_epoch);
    let begin_slot = previous_epoch * 432000;
    let end_slot = (previous_epoch + 1) * 432000;
    let mut block_slots = vec![];
    for i in (begin_slot..end_slot).step_by(1000) {
        let mut b = rpc_client.get_blocks(i, Some(i + 1000)).await?;
        block_slots.append(&mut b);
    }

    let (sender, reciever) = tokio::sync::mpsc::unbounded_channel::<(u64, UiConfirmedBlock)>();
    tokio::spawn(async move {
        // let mut writer =
        //     csv_async::AsyncSerializer::from_writer(File::create("out.csv").await.unwrap());
        let mut reciever = reciever;
        let postgres = Postgres::new().await;
        loop {
            let data = reciever.recv().await;
            match data {
                Some((slot, block)) => {
                    postgres.save_block(slot, block).await;
                    // writer.serialize(data).await.unwrap();
                    // writer.flush().await.unwrap();
                }
                None => break,
            }
        }
    });

    for blocks in block_slots.chunks(20) {
        let jhs = blocks
            .iter()
            .cloned()
            .map(|slot| {
                let rpc_client = rpc_client.clone();
                let sender = sender.clone();
                tokio::spawn(async move {
                    let block = rpc_client
                        .get_block_with_config(
                            slot,
                            solana_client::rpc_config::RpcBlockConfig {
                                encoding: Some(UiTransactionEncoding::Base64),
                                transaction_details: Some(
                                    solana_transaction_status::TransactionDetails::Full,
                                ),
                                rewards: Some(true),
                                commitment: None,
                                max_supported_transaction_version: Some(0),
                            },
                        )
                        .await;
                    if let Ok(block) = block {
                        let _= sender.send((slot, block));
                        // let block_height = block.block_height.unwrap_or_default();
                        // let nb_transactions = block.signatures.map_or(0, |x| x.len());
                        // let block_rewards = block
                        //     .rewards
                        //     .map(|x| {
                        //         x.iter()
                        //             .find(|y| y.reward_type == Some(RewardType::Fee))
                        //             .map(|x| x.lamports)
                        //             .unwrap_or_default()
                        //     })
                        //     .unwrap_or_default();
                        // sender
                        //     .send(BlockInfo {
                        //         block_height,
                        //         nb_transactions,
                        //         block_rewards,
                        //     })
                        //     .unwrap();
                    }
                })
            })
            .collect_vec();
        futures::future::join_all(jhs).await;
    }
    Ok(())
}
