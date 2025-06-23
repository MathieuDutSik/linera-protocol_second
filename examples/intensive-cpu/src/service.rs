// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::sync::Arc;

use intensive_cpu::{IntensiveCpuRequest, IntensiveCpuOperation};
use linera_sdk::{linera_base_types::WithServiceAbi, Service, ServiceRuntime};

use self::state::IntensiveCpuState;

pub struct IntensiveCpuService {
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(IntensiveCpuService);

impl WithServiceAbi for IntensiveCpuService {
    type Abi = intensive_cpu::IntensiveCpuAbi;
}

impl Service for IntensiveCpuService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        IntensiveCpuService {
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, request: IntensiveCpuRequest) {
        match request {
            IntensiveCpuRequest::PowerTest(n) => {
                IntensiveCpuState::power_test(n)
            },
            IntensiveCpuRequest::ScheduleDirectOperation(n) => {
                let operation = IntensiveCpuOperation::PowerTestDirect(n);
                self.runtime.schedule_operation(&operation);
            }
        }
    }
}
