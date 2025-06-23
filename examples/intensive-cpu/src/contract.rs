// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use intensive_cpu::{IntensiveCpuAbi, IntensiveCpuOperation};
use linera_sdk::{
    linera_base_types::WithContractAbi,
    Contract, ContractRuntime,
};

use self::state::IntensiveCpuState;

pub struct IntensiveCpuContract {
}

linera_sdk::contract!(IntensiveCpuContract);

impl WithContractAbi for IntensiveCpuContract {
    type Abi = IntensiveCpuAbi;
}

impl Contract for IntensiveCpuContract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = ();
    type EventValue = ();

    async fn load(_runtime: ContractRuntime<Self>) -> Self {
        IntensiveCpuContract { }
    }

    async fn instantiate(&mut self, _value: ()) {
    }

    async fn execute_operation(&mut self, operation: IntensiveCpuOperation) {
        let IntensiveCpuOperation::PowerTestDirect(n) = operation;
        IntensiveCpuState::power_test(n)
    }

    async fn execute_message(&mut self, _message: ()) {
        panic!("Counter application doesn't support any cross-chain messages");
    }

    async fn store(self) {
    }
}
