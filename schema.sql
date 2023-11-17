CREATE SCHEMA epoch_data;

CREATE TABLE epoch_data.transaction_infos (
  slot BIGINT,
  signature CHAR(88) PRIMARY KEY,
  is_executed_successfully INT,
  is_vote INT,
  vote_leader_identity CHAR(44),
  num_slots_voted INT,
  cu_requested BIGINT,
  prioritization_fees BIGINT,
  cu_consumed BIGINT,
  number_of_signatures INT,
  fee BIGINT,
);

CREATE TABLE epoch_data.blocks (
  block_hash CHAR(44) PRIMARY KEY,
  slot BIGINT,
  block_height BIGINT,
  leader_identity CHAR(44),
  processed_transactions BIGINT,
  successful_transactions BIGINT,
  total_cu_used BIGINT,
  total_cu_requested BIGINT,
  total_rewards BIGINT,
  total_prioritization_fees BIGINT,
);

CREATE TABLE epoch_data.rewards (
  slot BIGINT,
  reward_type INT,
  pubkey CHAR(44),
  lamports BIGINT,
  post_balance BIGINT,
  commission INT,
);