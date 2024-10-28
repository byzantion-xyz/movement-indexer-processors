use super::{DefaultProcessingResult, ProcessorName, ProcessorTrait};
use crate::{
    db::common::models::token_models::{
        collection_datas::CurrentCollectionData,
        token_datas::CurrentTokenData,
        token_ownerships::CurrentTokenOwnership,
        tokens::{
            CurrentTokenOwnershipPK, TableMetadataForToken, Token,
            TokenDataIdHash,
        },
    },
    schema,
    utils::database::{execute_in_chunks, get_config_table_chunk_size, ArcDbPool},
    IndexerGrpcProcessorConfig,
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
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tracing::error;
use crate::gap_detectors::ProcessingResult;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MercatoTokenProcessorConfig {
    pub nft_points_contract: Option<String>,
    #[serde(default = "IndexerGrpcProcessorConfig::default_query_retries")]
    pub query_retries: u32,
    #[serde(default = "IndexerGrpcProcessorConfig::default_query_retry_delay_ms")]
    pub query_retry_delay_ms: u64,
}

pub struct MercatoTokenProcessor {
    connection_pool: ArcDbPool,
    config: MercatoTokenProcessorConfig,
    per_table_chunk_sizes: AHashMap<String, usize>,
}

impl MercatoTokenProcessor {
    pub fn new(
        connection_pool: ArcDbPool,
        config: MercatoTokenProcessorConfig,
        per_table_chunk_sizes: AHashMap<String, usize>,
    ) -> Self {
        Self {
            connection_pool,
            config,
            per_table_chunk_sizes,
        }
    }
}

impl Debug for MercatoTokenProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "MercatoTokenTransactionProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

async fn insert_to_db(
    conn: ArcDbPool,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    (current_token_ownerships, current_token_datas, current_collection_datas): (
        &[CurrentTokenOwnership],
        &[CurrentTokenData],
        &[CurrentCollectionData],
    ),
    per_table_chunk_sizes: &AHashMap<String, usize>,
) -> Result<(), diesel::result::Error> {
    tracing::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );

    let cto = execute_in_chunks(
        conn.clone(),
        insert_current_token_ownerships_query,
        current_token_ownerships,
        get_config_table_chunk_size::<CurrentTokenOwnership>(
            "current_token_ownerships",
            per_table_chunk_sizes,
        ),
    );
    let ctd = execute_in_chunks(
        conn.clone(),
        insert_current_token_datas_query,
        current_token_datas,
        get_config_table_chunk_size::<CurrentTokenData>(
            "current_token_datas",
            per_table_chunk_sizes,
        ),
    );
    let ccd = execute_in_chunks(
        conn.clone(),
        insert_current_collection_datas_query,
        current_collection_datas,
        get_config_table_chunk_size::<CurrentCollectionData>(
            "current_collection_datas",
            per_table_chunk_sizes,
        ),
    );

    let (cto_res, ctd_res, ccd_res) =
        tokio::join!(cto, ctd, ccd);

    for res in [
        cto_res, ctd_res, ccd_res,
    ] {
        res?;
    }
    Ok(())
}

fn insert_current_token_ownerships_query(
    items_to_insert: Vec<CurrentTokenOwnership>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_token_ownerships::dsl::*;

    (diesel::insert_into(schema::current_token_ownerships::table)
         .values(items_to_insert)
         .on_conflict((token_data_id_hash, property_version, owner_address))
         .do_update()
         .set((
             creator_address.eq(excluded(creator_address)),
             collection_name.eq(excluded(collection_name)),
             name.eq(excluded(name)),
             amount.eq(excluded(amount)),
             token_properties.eq(excluded(token_properties)),
             last_transaction_version.eq(excluded(last_transaction_version)),
             collection_data_id_hash.eq(excluded(collection_data_id_hash)),
             table_type.eq(excluded(table_type)),
             inserted_at.eq(excluded(inserted_at)),
         )),
     Some(" WHERE current_token_ownerships.last_transaction_version <= excluded.last_transaction_version "),
    )
}

fn insert_current_token_datas_query(
    items_to_insert: Vec<CurrentTokenData>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_token_datas::dsl::*;
    (diesel::insert_into(schema::current_token_datas::table)
         .values(items_to_insert)
         .on_conflict(token_data_id_hash)
         .do_update()
         .set((
             creator_address.eq(excluded(creator_address)),
             collection_name.eq(excluded(collection_name)),
             name.eq(excluded(name)),
             maximum.eq(excluded(maximum)),
             supply.eq(excluded(supply)),
             largest_property_version.eq(excluded(largest_property_version)),
             metadata_uri.eq(excluded(metadata_uri)),
             payee_address.eq(excluded(payee_address)),
             royalty_points_numerator.eq(excluded(royalty_points_numerator)),
             royalty_points_denominator.eq(excluded(royalty_points_denominator)),
             maximum_mutable.eq(excluded(maximum_mutable)),
             uri_mutable.eq(excluded(uri_mutable)),
             description_mutable.eq(excluded(description_mutable)),
             properties_mutable.eq(excluded(properties_mutable)),
             royalty_mutable.eq(excluded(royalty_mutable)),
             default_properties.eq(excluded(default_properties)),
             last_transaction_version.eq(excluded(last_transaction_version)),
             collection_data_id_hash.eq(excluded(collection_data_id_hash)),
             description.eq(excluded(description)),
             inserted_at.eq(excluded(inserted_at)),
         )),
     Some(" WHERE current_token_datas.last_transaction_version <= excluded.last_transaction_version "),
    )
}

fn insert_current_collection_datas_query(
    items_to_insert: Vec<CurrentCollectionData>,
) -> (
    impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send,
    Option<&'static str>,
) {
    use schema::current_collection_datas::dsl::*;

    (diesel::insert_into(schema::current_collection_datas::table)
         .values(items_to_insert)
         .on_conflict(collection_data_id_hash)
         .do_update()
         .set((
             creator_address.eq(excluded(creator_address)),
             collection_name.eq(excluded(collection_name)),
             description.eq(excluded(description)),
             metadata_uri.eq(excluded(metadata_uri)),
             supply.eq(excluded(supply)),
             maximum.eq(excluded(maximum)),
             maximum_mutable.eq(excluded(maximum_mutable)),
             uri_mutable.eq(excluded(uri_mutable)),
             description_mutable.eq(excluded(description_mutable)),
             last_transaction_version.eq(excluded(last_transaction_version)),
             table_handle.eq(excluded(table_handle)),
             inserted_at.eq(excluded(inserted_at)),
         )),
     Some(" WHERE current_collection_datas.last_transaction_version <= excluded.last_transaction_version "),
    )
}


#[async_trait]
impl ProcessorTrait for MercatoTokenProcessor {
    fn name(&self) -> &'static str {
        ProcessorName::MercatoTokenProcessor.into()
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
        _: Option<u64>,
    ) -> anyhow::Result<ProcessingResult> {
        tracing::info!(
            name = self.name(),
            start_version = start_version,
            end_version = end_version,
            "Processing new transactions",
        );
        
        let processing_start = std::time::Instant::now();
        let last_transaction_timestamp = transactions.last().unwrap().timestamp.clone();

        let mut conn = self.get_conn().await;
        let query_retries = self.config.query_retries;
        let query_retry_delay_ms = self.config.query_retry_delay_ms;

        // First get all token related table metadata from the batch of transactions. This is in case
        // an earlier transaction has metadata (in resources) that's missing from a later transaction.
        let table_handle_to_owner =
            TableMetadataForToken::get_table_handle_to_owner_from_transactions(&transactions);

        // Hashmap key will be the PK of the table, we do not want to send duplicates writes to the db within a batch
        let mut all_current_token_ownerships: AHashMap<
            CurrentTokenOwnershipPK,
            CurrentTokenOwnership,
        > = AHashMap::new();
        let mut all_current_token_datas: AHashMap<TokenDataIdHash, CurrentTokenData> =
            AHashMap::new();
        let mut all_current_collection_datas: AHashMap<TokenDataIdHash, CurrentCollectionData> =
            AHashMap::new();

        for txn in &transactions {
            let (
                _a,
                _b,
                _c,
                _d,
                current_token_ownerships,
                current_token_datas,
                current_collection_datas,
            ) = Token::from_transaction(
                txn,
                &table_handle_to_owner,
                &mut conn,
                query_retries,
                query_retry_delay_ms,
            )
            .await;
            all_current_token_ownerships.extend(current_token_ownerships);
            all_current_token_datas.extend(current_token_datas);
            all_current_collection_datas.extend(current_collection_datas);
        }

        // Getting list of values and sorting by pk in order to avoid postgres deadlock since we're doing multi threaded db writes
        let mut all_current_token_ownerships = all_current_token_ownerships
            .into_values()
            .collect::<Vec<CurrentTokenOwnership>>();
        let mut all_current_token_datas = all_current_token_datas
            .into_values()
            .collect::<Vec<CurrentTokenData>>();
        let mut all_current_collection_datas = all_current_collection_datas
            .into_values()
            .collect::<Vec<CurrentCollectionData>>();

        // Sort by PK
        all_current_token_ownerships.sort_by(|a, b| {
            (&a.token_data_id_hash, &a.property_version, &a.owner_address).cmp(&(
                &b.token_data_id_hash,
                &b.property_version,
                &b.owner_address,
            ))
        });
        all_current_token_datas.sort_by(|a, b| a.token_data_id_hash.cmp(&b.token_data_id_hash));
        all_current_collection_datas
            .sort_by(|a, b| a.collection_data_id_hash.cmp(&b.collection_data_id_hash));


        let processing_duration_in_secs = processing_start.elapsed().as_secs_f64();
        let db_insertion_start = std::time::Instant::now();

        let tx_result = insert_to_db(
            self.get_pool(),
            self.name(),
            start_version,
            end_version,
            (
                &all_current_token_ownerships,
                &all_current_token_datas,
                &all_current_collection_datas,
            ),
            &self.per_table_chunk_sizes,
        )
        .await;

        let db_insertion_duration_in_secs = db_insertion_start.elapsed().as_secs_f64();
        tracing::info!(
            name = self.name(),
            start_version = start_version,
            end_version = end_version,
            "Finished processing new transactions",
        );
        match tx_result {
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
        }
    }

    fn connection_pool(&self) -> &ArcDbPool {
        &self.connection_pool
    }
}
