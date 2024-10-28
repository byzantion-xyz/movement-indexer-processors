#![allow(clippy::extra_unused_lifetimes)]
#![allow(clippy::unused_unit)]

use crate::db::common::models::account_transaction_models::account_transactions::AccountTransaction;
use crate::schema::accounts;
use aptos_protos::transaction::v1::Transaction;
use ahash::AHashMap;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, FieldCount, Identifiable, Insertable, Serialize)]
#[diesel(primary_key(account_address))]
#[diesel(table_name = accounts)]
pub struct Account {
    pub account_address: String,
}

impl Account {
    pub fn from_transaction(transaction: &Transaction) -> AHashMap<String, Self> {
        let account_transactions = AccountTransaction::from_transaction(transaction)
            .into_values()
            .collect::<Vec<AccountTransaction>>();
        let mut map = AHashMap::new();
        for tr in account_transactions {
            map.insert(tr.account_address.clone(), Self{
                account_address: tr.account_address.clone(),
            });
        }
        map
    }
}