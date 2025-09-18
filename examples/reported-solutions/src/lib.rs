// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

/*! ABI of the Reported Solutions Example Application */

use async_graphql::{Request, Response};
use linera_sdk::{
    graphql::GraphQLMutationRoot,
    linera_base_types::{ContractAbi, ServiceAbi},
};
use serde::{Deserialize, Serialize};

pub struct ReportedSolutionsAbi;

#[derive(Debug, Deserialize, Serialize, GraphQLMutationRoot)]
pub enum ReportedSolutionsOperation {
    /// Set a value in the nested collection
    SetSolution { key1: u16, key2: u32, value: u64 },
    /// Remove an entry from the inner collection
    RemoveInnerEntry { key1: u16, key2: u32 },
    /// Remove an entire outer collection entry
    RemoveOuterEntry { key1: u16 },
}

impl ContractAbi for ReportedSolutionsAbi {
    type Operation = ReportedSolutionsOperation;
    type Response = ();
}

impl ServiceAbi for ReportedSolutionsAbi {
    type Query = Request;
    type QueryResponse = Response;
}