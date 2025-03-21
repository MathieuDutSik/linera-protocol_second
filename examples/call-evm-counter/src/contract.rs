// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use linera_sdk::abis::evm::EvmAbi;
use alloy::primitives::U256;
use call_evm_counter::{CallCounter, CallCounterOperation};
use alloy_sol_types::{sol, SolCall};
use linera_sdk::{
    linera_base_types::{ApplicationId, WithContractAbi},
    Contract, ContractRuntime,
};

pub struct CallCounterContract {
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(CallCounterContract);

impl WithContractAbi for CallCounterContract {
    type Abi = CallCounter;
}

impl Contract for CallCounterContract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = ApplicationId<EvmAbi>;

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        CallCounterContract { runtime }
    }

    async fn instantiate(&mut self, _value: ()) {
        // Validate that the application parameters were configured correctly.
        self.runtime.application_parameters();
    }

    async fn execute_operation(&mut self, operation: CallCounterOperation) -> u64 {
        let CallCounterOperation::Increment(increment) = operation;
        sol! {
            function increment(uint256 input);
        }
        let input = U256::from(increment);
        let fct_args = incrementCall { input };
        let fct_args = fct_args.abi_encode().into();
        let evm_counter_id = self.runtime.application_parameters();
        let result = self.runtime.call_application(true, evm_counter_id, &fct_args);
        let result = U256::from_be_slice(result.as_ref());
	let (result, _) = result.most_significant_bits();
        result
    }

    async fn execute_message(&mut self, _message: ()) {
        panic!("Counter application doesn't support any cross-chain messages");
    }

    async fn store(self) {
    }
}
