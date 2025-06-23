// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use std::sync::Arc;

use call_intensive::{CallIntensiveOperation, CallIntensiveRequest};
use intensive_cpu::{IntensiveCpuAbi, IntensiveCpuRequest};
use linera_sdk::{
    linera_base_types::{ApplicationId, WithServiceAbi},
    Service, ServiceRuntime,
};

pub struct CallIntensiveService {
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(CallIntensiveService);

impl WithServiceAbi for CallIntensiveService {
    type Abi = call_intensive::CallIntensiveAbi;
}

impl Service for CallIntensiveService {
    type Parameters = ApplicationId<IntensiveCpuAbi>;

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        CallIntensiveService {
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, request: CallIntensiveRequest) {
        let application_id = self.runtime.application_parameters();
        match request {
            CallIntensiveRequest::QueryProcess(n) => {
                let request = IntensiveCpuRequest::PowerTest(n);
                self.runtime.query_application(application_id, &request);
            }
            CallIntensiveRequest::ScheduleDirectOperation(n) => {
                let operation = CallIntensiveOperation::PowerTestDirect(n);
                self.runtime.schedule_operation(&operation);
            }
            CallIntensiveRequest::ScheduleServiceOperation(n) => {
                let operation = CallIntensiveOperation::PowerTestService(n);
                self.runtime.schedule_operation(&operation);
            }
        }
    }
}
