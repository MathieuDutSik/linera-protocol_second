// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Counter Example Application that does not use GraphQL */

use linera_sdk::linera_base_types::{ContractAbi, ServiceAbi};
use serde::{Deserialize, Serialize};

pub struct IntensiveCpuAbi;

impl ContractAbi for IntensiveCpuAbi {
    type Operation = IntensiveCpuOperation;
    type Response = ();
}

impl ServiceAbi for IntensiveCpuAbi {
    type Query = IntensiveCpuRequest;
    type QueryResponse = ();
}

#[derive(Debug, Serialize, Deserialize)]
pub enum IntensiveCpuRequest {
    PowerTest(u64),
    ScheduleDirectOperation(u64),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum IntensiveCpuOperation {
    PowerTestDirect(u64),
}
