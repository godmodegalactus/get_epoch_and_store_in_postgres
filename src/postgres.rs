use std::sync::Arc;

use anyhow::Context;
use itertools::Itertools;
use solana_sdk::{compute_budget::{self, ComputeBudgetInstruction}, borsh0_10::try_from_slice_unchecked, vote, pubkey::Pubkey};
use solana_transaction_status::{EncodedTransactionWithStatusMeta, UiConfirmedBlock, RewardType, Reward};
use tokio_postgres::{tls::MakeTlsConnect, types::ToSql, Client, NoTls, Socket};

pub struct PostgresSession {
    pub client: Client,
}

impl PostgresSession {
    pub async fn new() -> anyhow::Result<Self> {
        let pg_config = std::env::var("PG_CONFIG").context("env PG_CONFIG not found")?;
        let pg_config = pg_config.parse::<tokio_postgres::Config>()?;

        let client = Self::spawn_connection(pg_config, NoTls).await?;

        Ok(Self { client })
    }

    async fn spawn_connection<T>(
        pg_config: tokio_postgres::Config,
        connector: T,
    ) -> anyhow::Result<Client>
    where
        T: MakeTlsConnect<Socket> + Send + 'static,
        <T as MakeTlsConnect<Socket>>::Stream: Send,
    {
        let (client, connection) = pg_config
            .connect(connector)
            .await
            .context("Connecting to Postgres failed")?;

        tokio::spawn(async move {
            log::info!("Connecting to Postgres");

            if let Err(err) = connection.await {
                log::error!("Connection to Postgres broke {err:?}");
                return;
            }
            unreachable!("Postgres thread returned")
        });

        Ok(client)
    }

    pub fn multiline_query(query: &mut String, args: usize, rows: usize, types: &[&str]) {
        let mut arg_index = 1usize;
        for row in 0..rows {
            query.push('(');

            for i in 0..args {
                if row == 0 && !types.is_empty() {
                    query.push_str(&format!("(${arg_index})::{}", types[i]));
                } else {
                    query.push_str(&format!("${arg_index}"));
                }
                arg_index += 1;
                if i != (args - 1) {
                    query.push(',');
                }
            }

            query.push(')');

            if row != (rows - 1) {
                query.push(',');
            }
        }
    }
}


pub struct Postgres {
    session: Arc<PostgresSession>,
}

impl Postgres {
    pub async fn new() -> Self {
        let session = PostgresSession::new().await.unwrap();
        Self {
            session: Arc::new(session),
        }
    }

    pub async fn save_banking_transaction_results(
        &self,
        txs: &Vec<TransactionRecord>,
    ) -> anyhow::Result<()> {
        if txs.is_empty() {
            return Ok(());
        }

        let mut args: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(11 * txs.len());
        for tx in txs.iter() {
            args.push(&tx.slot);
            args.push(&tx.signature);
            args.push(&tx.is_executed_successfully);
            args.push(&tx.is_vote);
            args.push(&tx.vote_leader_identity);
            args.push(&tx.num_slots_voted);
            args.push(&tx.cu_requested);
            args.push(&tx.prioritization_fees);
            args.push(&tx.cu_consumed);
            args.push(&tx.number_of_signatures);
            args.push(&tx.fee);
        }

        let mut query = String::from(
            r#"
                INSERT INTO epoch_data.transaction_infos 
                (slot, signature, is_executed_successfully, is_vote, vote_leader_identity, num_slots_voted, cu_requested, prioritization_fees, cu_consumed, number_of_signatures, fee)
                VALUES
            "#,
        );

        PostgresSession::multiline_query(&mut query, 11, txs.len(), &[]);
        self.session.client.execute(&query, &args).await?;

        Ok(())
    }

    pub async fn save_block_data(
        &self,
        block: &BlockRecord,
    ) -> anyhow::Result<()> {
        let mut args: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(10);
        args.push(&block.block_hash);
        args.push(&block.slot);
        args.push(&block.block_height);
        args.push(&block.leader_identity);
        args.push(&block.processed_transactions);
        args.push(&block.successful_transactions);
        args.push(&block.total_cu_used);
        args.push(&block.total_cu_requested);
        args.push(&block.total_rewards);
        args.push(&block.total_prioritization_fees);

        let mut query = String::from(
            r#"
                INSERT INTO epoch_data.blocks 
                (block_hash, slot, block_height, leader_identity, processed_transactions, processed_transactions, successful_transactions, total_cu_used, total_cu_requested, total_rewards, total_prioritization_fees)
                VALUES
            "#,
        );

        PostgresSession::multiline_query(&mut query, 10, 1, &[]);
        self.session.client.execute(&query, &args).await?;

        Ok(())
    }

    pub async fn save_rewards(&self, rewards: Vec<RewardInfo>) -> anyhow::Result<()> {
        if rewards.is_empty() {
            return Ok(());
        }

        let mut args: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(6 * rewards.len());
        for reward in rewards.iter() {
            args.push(&reward.slot);
            args.push(&reward.reward_type);
            args.push(&reward.pubkey);
            args.push(&reward.post_balance);
            args.push(&reward.commission);
            args.push(&reward.lamports);
        }

        let mut query = String::from(
            r#"
                INSERT INTO epoch_data.rewards 
                (slot, reward_type, pubkey, post_balance, commission, lamports)
                VALUES
            "#,
        );

        PostgresSession::multiline_query(&mut query, 6, rewards.len(), &[]);
        self.session.client.execute(&query, &args).await?;

        Ok(())
    }

    pub async fn save_block(&self, slot: u64, block : UiConfirmedBlock) {
        let txs = if let Some(txs) = block.transactions {
            txs
        } else {
            return;
        };
        let txs: Vec<TransactionRecord> = txs
        .iter()
        .filter_map(|x| TransactionRecord::from(slot, x))
        .collect();
        if let Err(e) = self.save_banking_transaction_results(&txs).await {
            log::error!("Error saving transaction {e:?}");
        }

        let rewards = match &block.rewards {
            Some(rewards) => rewards.iter().map(|reward| RewardInfo::from(slot, reward)).collect_vec(),
            None => vec![]
        };

        let total_cu_used = txs.iter().filter_map(|x| x.cu_consumed).sum();
        let total_cu_requested = txs.iter().filter_map(|x| x.cu_requested).sum();

        let (total_rewards, leader_identity) = block
                            .rewards
                            .map(|x| {
                                x.iter()
                                    .find(|y| y.reward_type == Some(RewardType::Fee))
                                    .map(|x| (x.lamports, x.pubkey.clone()))
                                    .unwrap_or_default()
                            })
                            .unwrap_or_default();

        let block_data = BlockRecord{
            block_hash: block.blockhash.clone(),
            slot: slot as i64,
            block_height: block.block_height.clone().unwrap_or_default() as i64,
            leader_identity,
            processed_transactions: txs.len() as i64,
            successful_transactions: txs.iter().filter(|x| x.is_executed_successfully == 1).count() as i64,
            total_cu_used,
            total_cu_requested,
            total_rewards,
            total_prioritization_fees: txs.iter().map(|x| x.prioritization_fees.unwrap_or_default() * x.cu_requested.unwrap_or(200000) as i64).sum(),
        };

        if let Err(e) = self.save_block_data(&block_data).await {
            log::error!("Error saving block {e:?}");
        }

        if let Err(e) = self.save_rewards(rewards).await {
            log::error!("Error saving rewards {e:?}");
        }

    }
}

pub struct TransactionRecord {
    pub slot: i64,
    pub signature: String,
    pub is_executed_successfully: i8,
    pub is_vote: i8,
    pub vote_leader_identity: Option<String>,
    pub num_slots_voted: Option<i16>,
    pub cu_requested: Option<i64>,
    pub prioritization_fees: Option<i64>,
    pub cu_consumed: Option<i64>,
    pub number_of_signatures: i8,
    pub fee: i64,
}

impl TransactionRecord {
    fn from( slot: u64, transaction: &EncodedTransactionWithStatusMeta) -> Option<Self> {
        let Some(meta) = &transaction.meta else {
            return None;
        };
        let transaction = if let Some(tx) = transaction.transaction.decode() {
            tx
        } else {
            return None
        };

        let legacy_compute_budget: Option<(u32, Option<u64>)> =
            transaction.message.instructions().iter().find_map(|i| {
                if i.program_id(transaction.message.static_account_keys())
                    .eq(&compute_budget::id())
                {
                    if let Ok(ComputeBudgetInstruction::RequestUnitsDeprecated {
                        units,
                        additional_fee,
                    }) = try_from_slice_unchecked(i.data.as_slice())
                    {
                        if additional_fee > 0 {
                            return Some((units, Some(((units * 1000) / additional_fee) as u64)));
                        } else {
                            return Some((units, None));
                        }
                    }
                }
                None
            });

        let legacy_cu_requested = legacy_compute_budget.map(|x| x.0);
        let legacy_prioritization_fees = legacy_compute_budget.map(|x| x.1).unwrap_or(None);


        let cu_requested = transaction.message
            .instructions()
            .iter()
            .find_map(|i| {
                if i.program_id(transaction.message.static_account_keys())
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

        let prioritization_fees = transaction.message
            .instructions()
            .iter()
            .find_map(|i| {
                if i.program_id(transaction.message.static_account_keys())
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
            .or(legacy_prioritization_fees);
        let cu_consumed = match meta.compute_units_consumed {
            solana_transaction_status::option_serializer::OptionSerializer::Some(x) => Some(x as i64),
            solana_transaction_status::option_serializer::OptionSerializer::None => None,
            solana_transaction_status::option_serializer::OptionSerializer::Skip => None,
        };

        let vote_data = transaction.message.instructions().iter().find_map(|i| 
        {
            if i.program_id(transaction.message.static_account_keys()).eq(&vote::program::id()) {
                if let Ok( vote::instruction::VoteInstruction::Vote(vote) ) = bincode::deserialize::<vote::instruction::VoteInstruction>(i.data.as_slice()) {
                    let vote_account = Pubkey::new(&i.accounts[3*32..3*32+32]);
                    return Some((vote_account.to_string(), vote.slots.len() as i16))
                }
            }
            None
        });
        Some(TransactionRecord {
            slot : slot as i64,
            signature: transaction.signatures[0].to_string(),
            is_executed_successfully: if meta.err.is_none() {1} else {0},
            is_vote: if vote_data.is_some() {1} else {0},
            vote_leader_identity: vote_data.as_ref().map(|x| x.0.clone()),
            cu_requested: cu_requested.map(|x| x as i64),
            prioritization_fees: prioritization_fees.map(|x| x as i64),
            cu_consumed,
            number_of_signatures: transaction.signatures.len() as i8,
            fee: meta.fee as i64,
            num_slots_voted: vote_data.map(|x| x.1)
        })
    }
}

pub struct BlockRecord {
    pub block_hash: String,
    pub slot: i64,
    pub block_height: i64,
    pub leader_identity : String,
    pub processed_transactions : i64,
    pub successful_transactions : i64,
    pub total_cu_used : i64,
    pub total_cu_requested : i64,
    pub total_rewards : i64,
    pub total_prioritization_fees : i64,
}

pub struct RewardInfo {
    pub slot: i64,
    pub reward_type: i8,
    pub pubkey: String,
    pub lamports: i64,
    pub post_balance: i64,
    pub commission : Option<i16>,
}

impl RewardInfo {
    pub fn from(slot: u64, reward: &Reward) -> Self {
        Self { 
            slot: slot as i64,
            pubkey: reward.pubkey.clone(), 
            lamports: reward.lamports, 
            post_balance: reward.post_balance as i64,
            reward_type: match reward.reward_type.unwrap_or(RewardType::Rent) {
                RewardType::Fee => 0,
                RewardType::Rent => 1,
                RewardType::Staking => 2,
                RewardType::Voting => 3,
            }, 
            commission: reward.commission.map(|x| x as i16) }
    }
}