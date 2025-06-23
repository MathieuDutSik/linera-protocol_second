// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use call_intensive::{CallIntensiveOperation, CallIntensiveAbi};
use intensive_cpu::{IntensiveCpuAbi, IntensiveCpuOperation, IntensiveCpuRequest};
use linera_sdk::{
    linera_base_types::{ApplicationId, WithContractAbi},
    Contract, ContractRuntime,
};

pub struct CallIntensiveContract {
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(CallIntensiveContract);

impl WithContractAbi for CallIntensiveContract {
    type Abi = CallIntensiveAbi;
}

impl Contract for CallIntensiveContract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = ApplicationId<IntensiveCpuAbi>;
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        CallIntensiveContract { runtime }
    }

    async fn instantiate(&mut self, _value: ()) {
    }

    async fn execute_operation(&mut self, operation: CallIntensiveOperation) {
        let application_id = self.runtime.application_parameters();
        match operation {
            CallIntensiveOperation::PowerTestDirect(n) => {
                let operation = IntensiveCpuOperation::PowerTestDirect(n);
                self.runtime
                    .call_application(true, application_id, &operation);
            }
            CallIntensiveOperation::PowerTestService(n) => {
                let request = IntensiveCpuRequest::PowerTest(n);
                self.runtime
                    .query_service(application_id, request);
            }
        }
    }

    async fn execute_message(&mut self, _message: ()) {
        panic!("Counter application doesn't support any cross-chain messages");
    }

    async fn store(self) {}
}
