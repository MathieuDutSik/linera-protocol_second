// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use std::sync::Arc;

use linera_sdk::abis::evm::EvmAbi;
use alloy::primitives::U256;
use alloy_sol_types::{sol, SolCall};
use call_evm_counter::{CallCounterOperation, CallCounterRequest};
use linera_sdk::{linera_base_types::{ApplicationId, WithServiceAbi}, Service, ServiceRuntime};

pub struct CallCounterService {
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(CallCounterService);

impl WithServiceAbi for CallCounterService {
    type Abi = call_evm_counter::CallCounter;
}

impl Service for CallCounterService {
    type Parameters = ApplicationId<EvmAbi>;

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        CallCounterService {
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, request: CallCounterRequest) -> u64 {
        match request {
            CallCounterRequest::Query => {
                sol! {
                    function get_value();
                }
                let request = get_valueCall { };
                let request = request.abi_encode().into();
                let evm_counter_id = self.runtime.application_parameters();
                let result = self.runtime.query_application(evm_counter_id, &request);
                let result = U256::from_be_slice(result.as_ref());
                let (result, _) = result.most_significant_bits();
                result
            },
            CallCounterRequest::Increment(value) => {
                let operation = CallCounterOperation::Increment(value);
                self.runtime.schedule_operation(&operation);
                0
            }
        }
    }
}
