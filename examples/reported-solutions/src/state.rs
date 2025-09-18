// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::views::{linera_views, CollectionView, RegisterView, RootView, ViewStorageContext};

/// The application state containing nested collections
#[derive(RootView, async_graphql::SimpleObject)]
#[view(context = ViewStorageContext)]
pub struct ReportedSolutionsState {
    pub reported_solutions: CollectionView<u16, CollectionView<u32, RegisterView<u64>>>,
}