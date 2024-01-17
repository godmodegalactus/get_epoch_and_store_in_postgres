CREATE SCHEMA epoch_data;

CREATE TABLE epoch_data.transaction_infos (
  slot BIGINT,
  signature text PRIMARY KEY,
  is_executed_successfully INT,
  is_vote INT,
  vote_leader_identity text,
  num_slots_voted INT,
  cu_requested BIGINT,
  prioritization_fees BIGINT,
  cu_consumed BIGINT,
  number_of_signatures INT,
  number_of_instructions INT,
  number_of_non_cb_instructions INT,
  fee BIGINT
);

CREATE TABLE epoch_data.blocks (
  block_hash text PRIMARY KEY,
  slot BIGINT,
  block_height BIGINT,
  leader_identity text,
  processed_transactions BIGINT,
  successful_transactions BIGINT,
  total_cu_used BIGINT,
  total_cu_requested BIGINT,
  total_rewards BIGINT,
  total_prioritization_fees BIGINT
);

CREATE TABLE epoch_data.rewards (
  slot BIGINT,
  reward_type INT,
  pubkey text,
  lamports BIGINT,
  post_balance BIGINT,
  commission INT
);