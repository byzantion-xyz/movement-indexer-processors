use super::{
    events_processor::EventsProcessor, /*mercato_account_processor::MercatoAccountProcessor, */user_transaction_processor::UserTransactionProcessor, ProcessorName, ProcessorTrait
};
use crate::{
    db::common::models::default_models::{block_metadata_transactions::BlockMetadataTransactionModel, transactions::TransactionModel},
    schema,
    utils::database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
    worker::TableFlags,
    db::common::models::default_models::move_resources::MoveResource,
    db::common::models::default_models::write_set_changes::WriteSetChangeDetail
};
use ahash::AHashMap;
use anyhow::bail;
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use diesel::{
    pg::{upsert::excluded, Pg},
    query_builder::QueryFragment,
    ExpressionMethods,
};
use std::fmt::Debug;
use tokio::join;
use tracing::error;
use crate::utils::counters::PROCESSOR_UNKNOWN_TYPE_COUNT;
use super::DefaultProcessingResult;
use crate::gap_detectors::ProcessingResult;

static INDEXED_RESOURCE_TYPES: &'static [&str] = &["0x4::royalty::Royalty"];
pub struct MercatoProcessor {
    connection_pool: ArcDbPool,
    per_table_chunk_sizes: AHashMap<String, usize>,
    events_processor: EventsProcessor,
    user_transaction_processor: UserTransactionProcessor,
    // account_processor: MercatoAccountProcessor,
}

impl MercatoProcessor {
    pub fn new(connection_pool: ArcDbPool, per_table_chunk_sizes: AHashMap<String, usize>,  deprecated_tables: TableFlags) -> Self {
        let events_processor_connection_pool = connection_pool.clone();
        let user_transaction_processor_connection_pool = connection_pool.clone();
        //let account_processor_pool = connection_pool.clone();
        let events_processor_per_table_chunk_sizes = per_table_chunk_sizes.clone();
        let user_transaction_processor_per_table_chunk_sizes = per_table_chunk_sizes.clone();
        //let account_processor_per_table_chunk_sizes = per_table_chunk_sizes.clone();
        Self {
            connection_pool,
            per_table_chunk_sizes,
            events_processor: EventsProcessor::new(
                events_processor_connection_pool,
                events_processor_per_table_chunk_sizes,
            ),
            user_transaction_processor: UserTransactionProcessor::new(
                user_transaction_processor_connection_pool,
                user_transaction_processor_per_table_chunk_sizes,
                deprecated_tables,
            ),
            // account_processor: MercatoAccountProcessor::new(
            //     account_processor_pool,
            //     account_processor_per_table_chunk_sizes,
            // ),
        }
    }
}

impl Debug for MercatoProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "DefaultTransactionProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

async fn insert_to_db(
    conn: ArcDbPool,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    txns: &[TransactionModel],
    move_resources: &[MoveResource],
    per_table_chunk_sizes: &AHashMap<String, usize>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting into \"transactions\"",
    );

    let txns_res = execute_in_chunks(
        conn.clone(),
        insert_transactions_query,
        txns,
        get_config_table_chunk_size::<TransactionModel>("transactions", per_table_chunk_sizes),
    );

    let mr_res = execute_in_chunks(
        conn.clone(),
        insert_move_resources_query,
        move_resources,
        get_config_table_chunk_size::<MoveResource>("move_resources", per_table_chunk_sizes),
    );

    let (txns_res,  mr_res) =
        join!(txns_res, mr_res);

    for res in [
        txns_res, mr_res
    ] {
        res?;
    }

    Ok(())
}

async fn insert_block_metadata_to_db(
    conn: ArcDbPool,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    block_metadata_transactions: &[BlockMetadataTransactionModel],
    per_table_chunk_sizes: &AHashMap<String, usize>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting into \"block_metadata_transactions\"",
    );

    execute_in_chunks(
        conn.clone(),
        insert_block_metadata_transactions_query,
        block_metadata_transactions,
        get_config_table_chunk_size::<BlockMetadataTransactionModel>(
            "block_metadata_transactions",
            per_table_chunk_sizes,
        ),
    ).await?;

    Ok(())
}

fn insert_transactions_query(
    items_to_insert: Vec<TransactionModel>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::transactions::dsl::*;

    (
        diesel::insert_into(schema::transactions::table)
            .values(items_to_insert)
            .on_conflict(version)
            .do_update()
            .set((
                inserted_at.eq(excluded(inserted_at)),
                payload_type.eq(excluded(payload_type)),
            )),
        None,
    )
}

fn insert_move_resources_query(
    items_to_insert: Vec<MoveResource>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::move_resources::dsl::*;

    (
        diesel::insert_into(schema::move_resources::table)
            .values(items_to_insert)
            .on_conflict((transaction_version, write_set_change_index))
            .do_nothing(),
        None,
    )
}

fn insert_block_metadata_transactions_query(
    items_to_insert: Vec<BlockMetadataTransactionModel>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::block_metadata_transactions::dsl::*;

    (
        diesel::insert_into(schema::block_metadata_transactions::table)
            .values(items_to_insert)
            .on_conflict(version)
            .do_nothing(),
        None,
    )
}

#[async_trait]
impl ProcessorTrait for MercatoProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::MercatoProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        for txn in &transactions {
            let txn_version = txn.version as i64;
            let _txn_data = match txn.txn_data.as_ref() {
                Some(txn_data) => txn_data,
                None => {
                    PROCESSOR_UNKNOWN_TYPE_COUNT
                        .with_label_values(&["UserTransactionProcessor"])
                        .inc();
                    tracing::warn!(
                        transaction_version = txn_version,
                        "Transaction data doesn't exist"
                    );
                    continue;
                },
            };
        }

        tracing::info!(
            name = self.name(),
            start_version = start_version,
            end_version = end_version,
            count = transactions.len(),
            "Processing new transactions",
        );

        if transactions.len() == 0 {
            return Ok(ProcessingResult::DefaultProcessingResult(DefaultProcessingResult {
                start_version,
                end_version,
                processing_duration_in_secs: 0.0,
                db_insertion_duration_in_secs: 0.0,
                last_transaction_timestamp: None,
            }));
        }

        let processing_start = std::time::Instant::now();
        let last_transaction_timestamp = transactions.last().unwrap().timestamp.clone();
        let (txns, block_metadata_txns, _, wsc_details) = TransactionModel::from_transactions(&transactions);
        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let mut block_metadata_transactions = vec![];
        for block_metadata_txn in block_metadata_txns {
            block_metadata_transactions.push(block_metadata_txn.clone());
        }

        let mut move_resources = vec![];
        for detail in wsc_details {
            match detail {
                WriteSetChangeDetail::Resource(resource) => {
                    if INDEXED_RESOURCE_TYPES.contains(&resource.type_.as_str()) {
                        move_resources.push(resource.clone());
                    }
                },
                _ => ()
            }
        }

        let tx_result = insert_to_db(
            self.get_pool(),
            self.name(),
            start_version,
            end_version,
            &txns,
            &move_resources,
            &self.per_table_chunk_sizes,
        )
            .await;

        let db_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();
        let result = match tx_result {
            Ok(_) => Ok(ProcessingResult::DefaultProcessingResult(DefaultProcessingResult {
                start_version,
                end_version,
                processing_duration_in_secs,
                db_insertion_duration_in_secs,
                last_transaction_timestamp,
            })),
            Err(e) => {
                error!(
                    start_version = start_version,
                    end_version = end_version,
                    processor_name = self.name(),
                    error = ?e,
                    "[Parser] Error inserting transactions to db",
                );
                bail!(e)
            },
        };
        tracing::trace!(
            name = self.name(),
            start_version = start_version,
            end_version = end_version,
            "Processing events",
        );
        self.events_processor
            .process_transactions(transactions.clone(), start_version, end_version, None)
            .await?;

        tracing::trace!(
            name = self.name(),
            start_version = start_version,
            end_version = end_version,
            "Processing user transactions",
        );
        self.user_transaction_processor
            .process_transactions(transactions.clone(), start_version, end_version, None)
            .await?;

        // tracing::trace!(
        //     name = self.name(),
        //     start_version = start_version,
        //     end_version = end_version,
        //     "Processing accounts",
        // );
        // self.account_processor
        //     .process_transactions(filtered_transactions, start_version, end_version, None)
        //     .await?;

        insert_block_metadata_to_db(
            self.get_pool(),
            self.name(),
            start_version,
            end_version,
            &block_metadata_transactions,
            &self.per_table_chunk_sizes,
        ).await?;
        tracing::info!(
            name = self.name(),
            start_version = start_version,
            end_version = end_version,
            "Finished processing new transactions",
        );
        result
    }

    fn connection_pool(&self) -> &ArcDbPool {
        &self.connection_pool
    }
}
