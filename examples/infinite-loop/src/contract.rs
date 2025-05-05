// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use infinite_loop::{InfiniteLoopAbi, InfiniteLoopOperation};
use linera_sdk::{
    linera_base_types::WithContractAbi,
    Contract, ContractRuntime,
};

pub struct InfiniteLoopContract;

linera_sdk::contract!(InfiniteLoopContract);

impl WithContractAbi for InfiniteLoopContract {
    type Abi = InfiniteLoopAbi;
}

impl Contract for InfiniteLoopContract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = ();
    type EventValue = ();

    async fn load(_runtime: ContractRuntime<Self>) -> Self {
        InfiniteLoopContract { }
    }

    async fn instantiate(&mut self, _value: ()) {
    }

    async fn execute_operation(&mut self, _operation: InfiniteLoopOperation) {
        panic!("Counter application doesn't support any operation");
    }

    async fn execute_message(&mut self, _message: ()) {
        panic!("Counter application doesn't support any cross-chain messages");
    }

    async fn store(self) {
    }
}
