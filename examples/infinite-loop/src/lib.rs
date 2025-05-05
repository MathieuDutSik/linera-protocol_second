// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Infinite loop test */

use linera_sdk::linera_base_types::{ContractAbi, ServiceAbi};
use serde::{Deserialize, Serialize};

pub struct InfiniteLoopAbi;

impl ContractAbi for InfiniteLoopAbi {
    type Operation = InfiniteLoopOperation;
    type Response = ();
}

impl ServiceAbi for InfiniteLoopAbi {
    type Query = InfiniteLoopRequest;
    type QueryResponse = u64;
}

#[derive(Debug, Serialize, Deserialize)]
pub enum InfiniteLoopRequest {
    Query,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum InfiniteLoopOperation {
    Increment(u64),
}
