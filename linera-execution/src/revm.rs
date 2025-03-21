// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Code specific to the usage of the [Revm](https://bluealloy.github.io/revm/) runtime.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    str::FromStr,
};

use alloy::primitives::{Address, B256, U256};
use linera_base::{crypto::CryptoHash, data_types::Bytecode, ensure, identifiers::{StreamName, UserApplicationId}};
use linera_views::common::from_bytes_option;
use revm::{
    db::{AccountState, WrapDatabaseRef},
    inspector_handle_register,
    primitives::{
        keccak256,
        state::{Account, AccountInfo},
        Bytes,
    },
    ContextPrecompile,
    ContextStatefulPrecompile,
    Database, DatabaseCommit, DatabaseRef, Evm, EvmContext,
    InnerEvmContext, Inspector,
};
use revm_primitives::{address, ExecutionResult, HaltReason, Log, Output, SuccessReason, TxKind};
use revm_interpreter::{CallInputs, CallOutcome, Gas, InstructionResult, InterpreterResult};
use revm_precompile::{PrecompileOutput, PrecompileResult, PrecompileErrors};
use core::ops::Range;
use thiserror::Error;
#[cfg(with_metrics)]
use {
    linera_base::prometheus_util::{
        exponential_bucket_latencies, register_histogram_vec, MeasureLatency as _,
    },
    prometheus::HistogramVec,
    std::sync::LazyLock,
};

use crate::{
    BaseRuntime, Batch, ContractRuntime, ContractSyncRuntimeHandle, EvmRuntime, ExecutionError,
    FinalizeContext, MessageContext, OperationContext, QueryContext, ServiceRuntime,
    ServiceSyncRuntimeHandle, UserContract, UserContractInstance, UserContractModule, UserService,
    UserServiceInstance, UserServiceModule, ViewError,
};

/// This is the selector of the `execute_message` that should be called
/// only from a submitted message
const EXECUTE_MESSAGE_SELECTOR: &[u8] = &[173, 125, 234, 205];

/// This the selector when calling for `InterpreterResult` this is a fictional
/// selector that does not correspond to a real function.
const INTERPRETER_RESULT_SELECTOR: &[u8] = &[1, 2, 3, 4];

#[cfg(test)]
mod tests {
    use revm_primitives::keccak256;

    use crate::revm::EXECUTE_MESSAGE_SELECTOR;

    // The function keccak256 is not const so we cannot build the execute_message
    // selector directly.
    #[test]
    fn check_execute_message_selector() {
        let selector = &keccak256("execute_message(bytes)".as_bytes())[..4];
        assert_eq!(selector, EXECUTE_MESSAGE_SELECTOR);
    }
}

#[cfg(with_metrics)]
static CONTRACT_INSTANTIATION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "evm_contract_instantiation_latency",
        "Evm contract instantiation latency",
        &[],
        exponential_bucket_latencies(1.0),
    )
});

#[cfg(with_metrics)]
static SERVICE_INSTANTIATION_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "evm_service_instantiation_latency",
        "Evm service instantiation latency",
        &[],
        exponential_bucket_latencies(1.0),
    )
});

#[derive(Debug, Error)]
pub enum EvmExecutionError {
    #[error("Failed to load contract EVM module: {_0}")]
    LoadContractModule(#[source] anyhow::Error),
    #[error("Failed to load service EVM module: {_0}")]
    LoadServiceModule(#[source] anyhow::Error),
    #[error("Commit error")]
    CommitError(String),
    #[error("It is illegal to call execute_message from an operation")]
    OperationCallExecuteMessage,
    #[error("The operation should contain the evm selector and so have length 4 or more")]
    OperationIsTooShort,
    #[error("Transact commit error")]
    TransactCommitError(String),
    #[error("The operation was reverted")]
    Revert {
        gas_used: u64,
        output: alloy::primitives::Bytes,
    },
    #[error("The operation was halted")]
    Halt { gas_used: u64, reason: HaltReason },
}

#[derive(Clone)]
pub enum EvmContractModule {
    #[cfg(with_revm)]
    Revm { module: Vec<u8> },
}

impl EvmContractModule {
    /// Creates a new [`EvmContractModule`] using the EVM module with the provided `contract_bytecode`.
    pub async fn new(
        contract_bytecode: Bytecode,
        runtime: EvmRuntime,
    ) -> Result<Self, EvmExecutionError> {
        match runtime {
            #[cfg(with_revm)]
            EvmRuntime::Revm => Self::from_revm(contract_bytecode).await,
        }
    }

    /// Creates a new [`EvmContractModule`] using the WebAssembly module in `contract_bytecode_file`.
    #[cfg(with_fs)]
    pub async fn from_file(
        contract_bytecode_file: impl AsRef<std::path::Path>,
        runtime: EvmRuntime,
    ) -> Result<Self, EvmExecutionError> {
        Self::new(
            Bytecode::load_from_file(contract_bytecode_file)
                .await
                .map_err(anyhow::Error::from)
                .map_err(EvmExecutionError::LoadContractModule)?,
            runtime,
        )
        .await
    }
}

impl UserContractModule for EvmContractModule {
    fn instantiate(
        &self,
        runtime: ContractSyncRuntimeHandle,
    ) -> Result<UserContractInstance, ExecutionError> {
//        panic!("Abort running fn instantiate");
        #[cfg(with_metrics)]
        let _instantiation_latency = CONTRACT_INSTANTIATION_LATENCY.measure_latency();

        let instance: UserContractInstance = match self {
            #[cfg(with_revm)]
            EvmContractModule::Revm { module } => {
                Box::new(RevmContractInstance::prepare(module.to_vec(), runtime)?)
            }
        };

        Ok(instance)
    }
}

impl EvmContractModule {
    /// Creates a new [`EvmContractModule`] using Revm with the provided bytecode files.
    pub async fn from_revm(contract_bytecode: Bytecode) -> Result<Self, EvmExecutionError> {
        let module = contract_bytecode.bytes;
        Ok(EvmContractModule::Revm { module })
    }
}

/// A user service in a compiled EVM module.
#[derive(Clone)]
pub enum EvmServiceModule {
    #[cfg(with_revm)]
    Revm { module: Vec<u8> },
}

impl EvmServiceModule {
    /// Creates a new [`EvmServiceModule`] using the EVM module with the provided bytecode.
    pub async fn new(
        service_bytecode: Bytecode,
        runtime: EvmRuntime,
    ) -> Result<Self, EvmExecutionError> {
        match runtime {
            #[cfg(with_revm)]
            EvmRuntime::Revm => Self::from_revm(service_bytecode).await,
        }
    }

    /// Creates a new [`EvmServiceModule`] using the EVM module in `service_bytecode_file`.
    #[cfg(with_fs)]
    pub async fn from_file(
        service_bytecode_file: impl AsRef<std::path::Path>,
        runtime: EvmRuntime,
    ) -> Result<Self, EvmExecutionError> {
        Self::new(
            Bytecode::load_from_file(service_bytecode_file)
                .await
                .map_err(anyhow::Error::from)
                .map_err(EvmExecutionError::LoadServiceModule)?,
            runtime,
        )
        .await
    }
}

impl UserServiceModule for EvmServiceModule {
    fn instantiate(
        &self,
        runtime: ServiceSyncRuntimeHandle,
    ) -> Result<UserServiceInstance, ExecutionError> {
        #[cfg(with_metrics)]
        let _instantiation_latency = SERVICE_INSTANTIATION_LATENCY.measure_latency();

        let instance: UserServiceInstance = match self {
            #[cfg(with_revm)]
            EvmServiceModule::Revm { module } => {
                Box::new(RevmServiceInstance::prepare(module.to_vec(), runtime)?)
            }
        };

        Ok(instance)
    }
}

impl EvmServiceModule {
    /// Creates a new [`EvmServiceModule`] using Revm with the provided bytecode files.
    pub async fn from_revm(contract_bytecode: Bytecode) -> Result<Self, EvmExecutionError> {
        let module = contract_bytecode.bytes;
        Ok(EvmServiceModule::Revm { module })
    }
}

//#[derive(Clone)]
struct DatabaseRuntime<Runtime> {
    commit_error: Option<Arc<ExecutionError>>,
    runtime: Arc<Mutex<Runtime>>,
}

impl<Runtime> Clone for DatabaseRuntime<Runtime> {
    fn clone(&self) -> Self {
        Self { commit_error: self.commit_error.clone(), runtime: self.runtime.clone() }
    }
}

#[repr(u8)]
pub enum KeyTag {
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

fn precompile_address() -> Address {
    address!("000000000000000000000000000000000000000b")
}

struct GeneralContractCall;

impl<Runtime: ContractRuntime> ContextStatefulPrecompile<WrapDatabaseRef<&mut DatabaseRuntime<Runtime>>> for GeneralContractCall {
    fn call(
        &self,
        input: &Bytes,
        _gas_limit: u64,
        context: &mut InnerEvmContext<WrapDatabaseRef<&mut DatabaseRuntime<Runtime>>>,
    ) -> PrecompileResult {
        let argument: Vec<u8> = input.to_vec();
        let result = {
            let mut runtime = context.db.0.runtime.lock().expect("The lock should be possible");
            let authenticated = true;
            let callee_id = runtime.application_id().map_err(|error| {
                PrecompileErrors::Fatal { msg: format!("{}", error) }
            })?;
            runtime.try_call_application(authenticated, callee_id, argument)
        }.map_err(|error| {
            PrecompileErrors::Fatal { msg: format!("{}", error) }
        })?;
        // We do not know how much gas was used.
        let gas_used = 0;
        let bytes = Bytes::copy_from_slice(&result);
        let result = PrecompileOutput { gas_used, bytes };
        Ok(result)
    }
}



struct CallInterceptorContract<Runtime> {
    db: DatabaseRuntime<Runtime>,
}


fn address_to_user_application_id(address: Address) -> UserApplicationId {
    let address : Vec<u8> = address.to_vec();
    let mut hash = [0_u8; 32];
    hash[..20].copy_from_slice(&address[..20]);
    let strout: String = hex::encode(hash);
    let application_description_hash = CryptoHash::from_str(&strout).unwrap();
    UserApplicationId::new(application_description_hash)
}

fn failing_outcome() -> CallOutcome {
    let result = InstructionResult::Revert;
    let output = Bytes::default();
    let gas = Gas::default();
    let result = InterpreterResult { result, output, gas };
    let memory_offset = Range::default();
    CallOutcome { result, memory_offset }
}

impl<Runtime: ContractRuntime> Inspector<WrapDatabaseRef<&mut DatabaseRuntime<Runtime>>> for CallInterceptorContract<Runtime> {
    fn call(
        &mut self,
        context: &mut EvmContext<WrapDatabaseRef<&mut DatabaseRuntime<Runtime>>>,
        inputs: &mut CallInputs,
    ) -> Option<CallOutcome> {
        let result = self.call_or_fail(context, inputs);
        match result {
            Err(_error) => {
                // An alternative way would be to return None, which would induce
                // REVM to call the smart contract in its database, where it is
                // non-existent.
                Some(failing_outcome())
            }
            Ok(result) => {
                result
            }
        }
    }
}

impl<Runtime: ContractRuntime> CallInterceptorContract<Runtime> {
    fn call_or_fail(
        &mut self,
        _context: &mut EvmContext<WrapDatabaseRef<&mut DatabaseRuntime<Runtime>>>,
        inputs: &mut CallInputs,
    ) -> Result<Option<CallOutcome>, ExecutionError> {
        let contract_address = Address::ZERO.create(0);

        if inputs.target_address != precompile_address() && inputs.target_address != contract_address {
            let mut argument: Vec<u8> = INTERPRETER_RESULT_SELECTOR.to_vec();
            argument.extend(inputs.input.to_vec());
            let authenticated = true;
            let callee_id = address_to_user_application_id(inputs.caller);
            let result = {
                let mut runtime = self.db.runtime.lock().expect("The lock should be possible");
                runtime.try_call_application(authenticated, callee_id, argument)?
            };
            let result = bcs::from_bytes::<InterpreterResult>(&result)?;
            let call_outcome = CallOutcome { result, memory_offset: inputs.return_memory_offset.clone() };
            Ok(Some(call_outcome))
        } else {
            Ok(None)
        }
    }
}

impl<Runtime: BaseRuntime> DatabaseRuntime<Runtime> {
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

    fn new(runtime: Runtime) -> Result<Self, ExecutionError> {
        Ok(Self {
            commit_error: None,
            runtime: Arc::new(Mutex::new(runtime)),
        })
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
                        batch.put_key_value(key, &value.present_value())?;
                    }
                }
            } else {
                if !account.storage.is_empty() {
                    panic!("For user account, storage must be empty");
                }
                // The only allowed operations are the ones for the
                // account balances.
                let new_balance = (address, account.info.balance);
                list_new_balances.push(new_balance);
            }
        }
        runtime.write_batch(batch)?;
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
            return Ok(account_info);
        }
        panic!("only contract address are supported thus far address={address:?}");
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

pub struct RevmContractInstance<Runtime> {
    module: Vec<u8>,
    db: DatabaseRuntime<Runtime>,
}

enum Choice {
    Create,
    Call,
}

impl<Runtime> UserContract for RevmContractInstance<Runtime>
where
    Runtime: ContractRuntime,
{
    fn instantiate(
        &mut self,
        _context: OperationContext,
        argument: Vec<u8>,
    ) -> Result<(), ExecutionError> {
//        panic!("Abort running instantiate");
        tracing::info!("instantiate, step 1");
        let argument = serde_json::from_slice::<Vec<u8>>(&argument)?;
        tracing::info!("instantiate, step 2");
        let mut vec = self.module.clone();
        tracing::info!("instantiate, step 3");
        vec.extend_from_slice(&argument);
        tracing::info!("instantiate, step 4");
        let tx_data = Bytes::copy_from_slice(&vec);
        tracing::info!("instantiate, step 5");
        let result = self.transact_commit_tx_data(Choice::Create, tx_data)?;
        tracing::info!("instantiate, step 6");
        let result = process_execution_result(result)?;
        tracing::info!("instantiate, step 7");
        self.write_logs(result.logs, "deploy")?;
        tracing::info!("instantiate, step 9");
        let Output::Create(_, _) = result.output else {
            unreachable!("It is impossible for a Choice::Create to lead to an Output::Call");
        };
        tracing::info!("instantiate, step 10");
        Ok(())
    }

    fn execute_operation(
        &mut self,
        _context: OperationContext,
        operation: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
//        panic!("Abort running execute_operation");
        tracing::info!("execute_operation, step 1");
        ensure!(
            operation.len() >= 4,
            ExecutionError::EvmError(EvmExecutionError::OperationIsTooShort)
        );
        tracing::info!("execute_operation, step 2");
        ensure!(
            &operation[..4] != EXECUTE_MESSAGE_SELECTOR,
            ExecutionError::EvmError(EvmExecutionError::OperationCallExecuteMessage)
        );
        tracing::info!("execute_operation, step 3");
        let (output, logs) = if &operation[..4] == INTERPRETER_RESULT_SELECTOR {
            ensure!(
                &operation[4..8] != EXECUTE_MESSAGE_SELECTOR,
                ExecutionError::EvmError(EvmExecutionError::OperationCallExecuteMessage)
            );
            let tx_data = Bytes::copy_from_slice(&operation[4..]);
            let result = self.transact_commit_tx_data(Choice::Call, tx_data)?;
            let result = process_execution_result(result)?;
            result.interpreter_result_and_logs()?
        } else {
            let tx_data = Bytes::copy_from_slice(&operation);
            let result = self.transact_commit_tx_data(Choice::Call, tx_data)?;
            let result = process_execution_result(result)?;
            result.output_and_logs()
        };
        tracing::info!("execute_operation, step 5");
        self.write_logs(logs, "operation")?;
        tracing::info!("execute_operation, step 6");
        Ok(output)
    }

    fn execute_message(
        &mut self,
        _context: MessageContext,
        _message: Vec<u8>,
    ) -> Result<(), ExecutionError> {
        panic!("The execute_message part of the Ethereum smart contract has not yet been coded");
    }

    fn finalize(&mut self, _context: FinalizeContext) -> Result<(), ExecutionError> {
        Ok(())
    }
}

struct ExecutionResultSuccess {
    reason: SuccessReason,
    logs: Vec<Log>,
    output: Output,
}

impl ExecutionResultSuccess {
    fn interpreter_result_and_logs(self) -> Result<(Vec<u8>, Vec<Log>), ExecutionError> {
        let result: InstructionResult = self.reason.into();
        let Output::Call(output) = self.output else {
            unreachable!("The Output is not a call which is impossible");
        };
        let gas = Gas::new(u64::MAX);
        let result = InterpreterResult { result, output, gas };
        let result = bcs::to_bytes(&result)?;
        Ok((result, self.logs))
    }

    fn output_and_logs(self) -> (Vec<u8>, Vec<Log>) {
        let Output::Call(output) = self.output else {
            unreachable!("It is impossible for a Choice::Call to lead to an Output::Create");
        };
        let output = output.as_ref().to_vec();
        (output, self.logs)
    }

}



fn process_execution_result(result: ExecutionResult) -> Result<ExecutionResultSuccess, ExecutionError> {
    match result {
        ExecutionResult::Success {
            reason,
            gas_used: _,
            gas_refunded: _,
            logs,
            output,
        } => Ok(ExecutionResultSuccess { reason, logs, output }),
        ExecutionResult::Revert { gas_used, output } => {
            let error = EvmExecutionError::Revert { gas_used, output };
            Err(ExecutionError::EvmError(error))
        }
        ExecutionResult::Halt { gas_used, reason } => {
            let error = EvmExecutionError::Halt { gas_used, reason };
            Err(ExecutionError::EvmError(error))
        }
    }
}

impl<Runtime> RevmContractInstance<Runtime>
where
    Runtime: ContractRuntime,
{
    pub fn prepare(module: Vec<u8>, runtime: Runtime) -> Result<Self, ExecutionError> {
        let db = DatabaseRuntime::new(runtime)?;
        Ok(Self { module, db })
    }

    fn transact_commit_tx_data(
        &mut self,
        ch: Choice,
        tx_data: Bytes,
    ) -> Result<ExecutionResult, ExecutionError> {
//        panic!("Abort running transact_commit_tx_data");
        tracing::info!("transact_commit_tx_data, step 1");
        // We use a fictional smart contract
        let contract_address = Address::ZERO.create(0);
        tracing::info!("transact_commit_tx_data, step 2");
        let kind = match ch {
            Choice::Create => TxKind::Create,
            Choice::Call => TxKind::Call(contract_address),
        };
        tracing::info!("transact_commit_tx_data, step 3");
        let mut inspector = CallInterceptorContract { db: self.db.clone() };
        tracing::info!("transact_commit_tx_data, step 4");
        let mut evm: Evm<'_, _, _> = Evm::builder()
            .with_ref_db(&mut self.db)
            .with_external_context(&mut inspector)
            .modify_tx_env(|tx| {
                tx.clear();
                tx.transact_to = kind;
                tx.data = tx_data;
            })
            .append_handler_register(|handler| {
                inspector_handle_register(handler);
                let precompiles = handler.pre_execution.load_precompiles();
                handler.pre_execution.load_precompiles = Arc::new(move || {
                    let mut precompiles = precompiles.clone();
                    precompiles.extend([(
                        precompile_address(),
                        ContextPrecompile::ContextStateful(Arc::new(GeneralContractCall)),
                    )]);
                    precompiles
                });
            })
            .build();
        tracing::info!("transact_commit_tx_data, step 5");

        evm.transact_commit().map_err(|error| {
            let error = format!("{:?}", error);
            let error = EvmExecutionError::TransactCommitError(error);
            ExecutionError::EvmError(error)
        })
    }

    fn write_logs(
        &mut self,
        logs: Vec<Log>,
        origin: &str,
    ) -> Result<(), ExecutionError> {
        if !logs.is_empty() {
            let mut runtime = self.db.runtime.lock().expect("The lock should be possible");
            let application_id = runtime.application_id()?;
            let stream_name = bcs::to_bytes("ethereum_event")?;
            let stream_name = StreamName(stream_name);
            for (log, index) in logs.iter().enumerate() {
                let mut key = bcs::to_bytes(&application_id)?;
                bcs::serialize_into(&mut key, origin)?;
                bcs::serialize_into(&mut key, index)?;
                let value = bcs::to_bytes(&log)?;
                runtime.emit(stream_name.clone(), key, value)?;
            }
        }
        Ok(())
    }
}

pub struct RevmServiceInstance<Runtime> {
    db: DatabaseRuntime<Runtime>,
}

impl<Runtime> RevmServiceInstance<Runtime>
where
    Runtime: ServiceRuntime,
{
    pub fn prepare(_module: Vec<u8>, runtime: Runtime) -> Result<Self, ExecutionError> {
        let db = DatabaseRuntime::new(runtime)?;
        Ok(Self { db })
    }
}

impl<Runtime> UserService for RevmServiceInstance<Runtime>
where
    Runtime: ServiceRuntime,
{
    fn handle_query(
        &mut self,
        _context: QueryContext,
        argument: Vec<u8>,
    ) -> Result<Vec<u8>, ExecutionError> {
//        panic!("Abort running handle_query");
        tracing::info!("handle_query, step 1");
        let argument: serde_json::Value = serde_json::from_slice(&argument)?;
        tracing::info!("handle_query, step 2");
        let argument = argument["query"].to_string();
        tracing::info!("handle_query, step 3");
        if let Some(residual) = argument.strip_prefix("\"mutation { v") {
            let operation = &residual[0..residual.len() - 3];
            let operation = hex::decode(operation).unwrap();
            let mut runtime = self.db.runtime.lock().expect("The lock should be possible");
            runtime.schedule_operation(operation)?;
            let answer = serde_json::json!({"data": ""});
            let answer = serde_json::to_vec(&answer).unwrap();
            return Ok(answer);
        }
        tracing::info!("handle_query, step 4");
        let argument = argument[10..argument.len() - 3].to_string();
        tracing::info!("handle_query, step 5");
        let argument = hex::decode(&argument).unwrap();
        tracing::info!("handle_query, step 6");
        let tx_data = Bytes::copy_from_slice(&argument);
        tracing::info!("handle_query, step 7");
        let address = Address::ZERO.create(0);
        tracing::info!("handle_query, step 8");
        let mut evm: Evm<'_, (), _> = Evm::builder()
            .with_ref_db(&mut self.db)
            .modify_tx_env(|tx| {
                tx.clear();
                tx.transact_to = TxKind::Call(address);
                tx.data = tx_data;
            })
            .build();
        tracing::info!("handle_query, step 9");

        let result_state = evm.transact().map_err(|error| {
            let error = format!("{:?}", error);
            let error = EvmExecutionError::TransactCommitError(error);
            ExecutionError::EvmError(error)
        })?;
        tracing::info!("handle_query, step 10");
        let result = process_execution_result(result_state.result)?;
        tracing::info!("handle_query, step 11");
        let (output, _logs) = result.output_and_logs();
        tracing::info!("handle_query, step 12");
        // We drop the logs since the "eth_call" execution does not return any log.
        let answer = hex::encode(&output);
        tracing::info!("handle_query, step 13");
        let answer: serde_json::Value = serde_json::json!({"data": answer});
        tracing::info!("handle_query, step 14");
        let answer = serde_json::to_vec(&answer).unwrap();
        tracing::info!("handle_query, step 15");
        Ok(answer)
    }
}
