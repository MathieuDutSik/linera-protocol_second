// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Revm](https://bluealloy.github.io/revm/) runtime.
//! Here we implement the Database traits of Revm.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use alloy::primitives::{Address, B256, U256};
use linera_views::common::from_bytes_option;
use revm::{
    db::AccountState,
    primitives::{
        keccak256,
        state::{Account, AccountInfo},
    },
    Database, DatabaseCommit, DatabaseRef,
};

use crate::{BaseRuntime, Batch, ContractRuntime, EvmExecutionError, ExecutionError, ViewError};

/// The cost of loading from storage
const SLOAD_COST: u64 = 2100;

/// The cost of storing a non-zero value in the storage
const SSTORE_COST_SET: u64 = 20000;

/// The cost of storing a zero value in the storage
const SSTORE_COST_SET_ZERO: u64 = 100;

/// The cost of storing the storage to the same value
const SSTORE_COST_RESET_EQ: u64 = 100;

/// The cost of storing the storage to a different value
const SSTORE_COST_RESET_NEQ: u64 = 2900;

/// The refund from releasing data
const SSTORE_REFUND_RELEASE: u64 = 4800;

#[derive(Clone, Default)]
struct StorageStats {
    number_key_reset_eq: u64,
    number_key_reset_neq: u64,
    number_key_set: u64,
    number_key_set_zero: u64,
    number_key_release: u64,
    number_key_read: u64,
}

impl StorageStats {
    fn storage_costs(&self) -> u64 {
        let mut storage_costs = 0;
        storage_costs += self.number_key_reset_eq * SSTORE_COST_RESET_EQ;
        storage_costs += self.number_key_reset_neq * SSTORE_COST_RESET_NEQ;
        storage_costs += self.number_key_set * SSTORE_COST_SET;
        storage_costs += self.number_key_set_zero * SSTORE_COST_SET_ZERO;
        storage_costs += self.number_key_read * SLOAD_COST;
        storage_costs
    }

    fn storage_refund(&self) -> u64 {
        seld.number_key_release * SSTORE_REFUND_RELEASE
    }
}



pub(crate) struct DatabaseRuntime<Runtime> {
    commit_error: Option<Arc<ExecutionError>>,
    storage_stats: Arc<Mutex<StorageStats>>,
    pub runtime: Arc<Mutex<Runtime>>,
}

impl<Runtime> Clone for DatabaseRuntime<Runtime> {
    fn clone(&self) -> Self {
        Self {
            commit_error: self.commit_error.clone(),
            runtime: self.runtime.clone(),
        }
    }
}

#[repr(u8)]
enum KeyTag {
    /// Key prefix for the storage of the zero contract.
    ZeroContractAddress,
    /// Key prefix for the storage of the contract address.
    ContractAddress,
}

#[repr(u8)]
pub enum KeyCategory {
    AccountInfo,
    AccountState,
    Storage,
}

impl<Runtime> DatabaseRuntime<Runtime> {
    fn get_uint256_key(val: u8, index: U256) -> Result<Vec<u8>, ExecutionError> {
        let mut key = vec![val, KeyCategory::Storage as u8];
        bcs::serialize_into(&mut key, &index)?;
        Ok(key)
    }

    fn get_contract_address_key(&self, address: &Address) -> Option<u8> {
        if address == &Address::ZERO {
            return Some(KeyTag::ZeroContractAddress as u8);
        }
        if address == &Address::ZERO.create(0) {
            return Some(KeyTag::ContractAddress as u8);
        }
        None
    }

    pub fn new(runtime: Runtime) -> Self {
        let storage_stats = StorageStats::default();
        Self {
            commit_error: None,
            storage_stats: Arc::new(Mutex::new(storage_stats)),
            runtime: Arc::new(Mutex::new(runtime)),
        }
    }

    fn reset_storage_stats(&self) -> StorageStats {
        let mut storage_stats_read = self
            .storage_stats
            .lock()
            .expect("The lock should be possible");
        let storage_stats = storage_stats_read.clone();
        *storage_stats_read = StorageStats::default();
        storage_stats
    }

    fn throw_error(&self) -> Result<(), ExecutionError> {
        if let Some(error) = &self.commit_error {
            let error = format!("{:?}", error);
            let error = EvmExecutionError::CommitError(error);
            return Err(ExecutionError::EvmError(error));
        }
        Ok(())
    }
}

impl<Runtime> Database for DatabaseRuntime<Runtime>
where
    Runtime: BaseRuntime,
{
    type Error = ExecutionError;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, ExecutionError> {
        self.basic_ref(address)
    }

    fn code_by_hash(
        &mut self,
        _code_hash: B256,
    ) -> Result<revm::primitives::Bytecode, ExecutionError> {
        panic!("Functionality code_by_hash not implemented");
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, ExecutionError> {
        self.storage_ref(address, index)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, ExecutionError> {
        self.throw_error()?;
        <Self as DatabaseRef>::block_hash_ref(self, number)
    }
}

impl<Runtime> DatabaseCommit for DatabaseRuntime<Runtime>
where
    Runtime: ContractRuntime,
{
    fn commit(&mut self, changes: HashMap<Address, Account>) {
        let result = self.commit_with_error(changes);
        if let Err(error) = result {
            self.commit_error = Some(error.into());
        }
    }
}

impl<Runtime> DatabaseRuntime<Runtime>
where
    Runtime: ContractRuntime,
{
    fn commit_with_error(
        &mut self,
        changes: HashMap<Address, Account>,
    ) -> Result<(), ExecutionError> {
        let mut number_key_reset_eq = 0;
        let mut number_key_reset_neq = 0;
        let mut number_key_set = 0;
        let mut number_key_set_zero = 0;
        let mut number_key_release = 0;
        let mut runtime = self.runtime.lock().expect("The lock should be possible");
        let mut batch = Batch::new();
        let mut list_new_balances = Vec::new();
        for (address, account) in changes {
            if !account.is_touched() {
                continue;
            }
            let val = self.get_contract_address_key(&address);
            if let Some(val) = val {
                let key_prefix = vec![val, KeyCategory::Storage as u8];
                let key_info = vec![val, KeyCategory::AccountInfo as u8];
                let key_state = vec![val, KeyCategory::AccountState as u8];
                if account.is_selfdestructed() {
                    batch.delete_key_prefix(key_prefix);
                    batch.put_key_value(key_info, &AccountInfo::default())?;
                    batch.put_key_value(key_state, &AccountState::NotExisting)?;
                } else {
                    let is_newly_created = account.is_created();
                    batch.put_key_value(key_info, &account.info)?;

                    let account_state = if is_newly_created {
                        batch.delete_key_prefix(key_prefix);
                        AccountState::StorageCleared
                    } else {
                        let promise = runtime.read_value_bytes_new(key_state.clone())?;
                        let result = runtime.read_value_bytes_wait(&promise)?;
                        let account_state = from_bytes_option::<AccountState, ViewError>(&result)?
                            .unwrap_or_default();
                        if account_state.is_storage_cleared() {
                            AccountState::StorageCleared
                        } else {
                            AccountState::Touched
                        }
                    };
                    batch.put_key_value(key_state, &account_state)?;
                    for (index, value) in account.storage {
                        let key = Self::get_uint256_key(val, index)?;
                        if value.original_value() == U256::ZERO {
                            if value.present_value() != U256::ZERO {
                                batch.put_key_value(key, &value.present_value())?;
                                number_key_set += 1;
                            } else {
                                number_key_set_zero += 1;
                            }
                        } else if value.present_value() != U256::ZERO {
                            if value.present_value() == value.original_value() {
                                number_key_reset_eq += 1;
                            } else {
                                batch.put_key_value(key, &value.present_value())?;
                                number_key_reset_neq += 1;
                            }
                        } else {
                            batch.delete_key(key);
                            number_key_release += 1;
                        }
                    }
                }
            } else {
                if !account.storage.is_empty() {
                    panic!("For user account, storage must be empty");
                }
                // TODO(#3756): Implement EVM transferts within Linera.
                // The only allowed operations are the ones for the
                // account balances.
                if account.info.balance != U256::ZERO {
                    let new_balance = (address, account.info.balance);
                    list_new_balances.push(new_balance);
                }
            }
        }
        runtime.write_batch(batch)?;
        let mut storage_stats = self
            .storage_stats
            .lock()
            .expect("The lock should be possible");
        storage_stats.number_key_reset_eq += number_key_reset_eq;
        storage_stats.number_key_reset_neq += number_key_reset_neq;
        storage_stats.number_key_set += number_key_set;
        storage_stats.number_key_set_zero += number_key_set_zero;
        storage_stats.number_key_release += number_key_release;
        if !list_new_balances.is_empty() {
            panic!("The conversion Ethereum address / Linera address is not yet implemented");
        }
        Ok(())
    }
}

impl<Runtime> DatabaseRef for DatabaseRuntime<Runtime>
where
    Runtime: BaseRuntime,
{
    type Error = ExecutionError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, ExecutionError> {
        self.throw_error()?;
        let mut runtime = self.runtime.lock().expect("The lock should be possible");
        let val = self.get_contract_address_key(&address);
        if let Some(val) = val {
            let key = vec![val, KeyCategory::AccountInfo as u8];
            let promise = runtime.read_value_bytes_new(key)?;
            let result = runtime.read_value_bytes_wait(&promise)?;
            let account_info = from_bytes_option::<AccountInfo, ViewError>(&result)?;
            Ok(account_info)
        } else {
            Ok(Some(AccountInfo::default()))
        }
    }

    fn code_by_hash_ref(
        &self,
        _code_hash: B256,
    ) -> Result<revm::primitives::Bytecode, ExecutionError> {
        panic!("Functionality code_by_hash_ref not implemented");
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, ExecutionError> {
        self.throw_error()?;
        let val = self.get_contract_address_key(&address);
        let Some(val) = val else {
            panic!("There is no storage associated to Externally Owned Account");
        };
        let key = Self::get_uint256_key(val, index)?;
        {
            let mut storage_stats = self
                .storage_stats
                .lock()
                .expect("The lock should be possible");
            storage_stats.number_key_read += 1;
        }
        let result = {
            let mut runtime = self.runtime.lock().expect("The lock should be possible");
            let promise = runtime.read_value_bytes_new(key)?;
            runtime.read_value_bytes_wait(&promise)
        }?;
        Ok(from_bytes_option::<U256, ViewError>(&result)?.unwrap_or_default())
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, ExecutionError> {
        self.throw_error()?;
        Ok(keccak256(number.to_string().as_bytes()))
    }
}
