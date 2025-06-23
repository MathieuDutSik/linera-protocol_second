// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Counter Example Application that does not use GraphQL */

use linera_sdk::linera_base_types::{ContractAbi, ServiceAbi};
use serde::{Deserialize, Serialize};

pub struct CallIntensiveAbi;

impl ContractAbi for CallIntensiveAbi {
    type Operation = CallIntensiveOperation;
    type Response = ();
}

impl ServiceAbi for CallIntensiveAbi {
    type Query = CallIntensiveRequest;
    type QueryResponse = ();
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CallIntensiveRequest {
    QueryProcess(u64),
    ScheduleDirectOperation(u64),
    ScheduleServiceOperation(u64),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CallIntensiveOperation {
    PowerTestDirect(u64),
    PowerTestService(u64),
}
