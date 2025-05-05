// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

use infinite_loop::InfiniteLoopRequest;
use linera_sdk::{linera_base_types::WithServiceAbi, Service, ServiceRuntime};

pub struct InfiniteLoopService;

linera_sdk::service!(InfiniteLoopService);

impl WithServiceAbi for InfiniteLoopService {
    type Abi = infinite_loop::InfiniteLoopAbi;
}

impl Service for InfiniteLoopService {
    type Parameters = ();

    async fn new(_runtime: ServiceRuntime<Self>) -> Self {
        InfiniteLoopService { }
    }

    async fn handle_query(&self, request: InfiniteLoopRequest) -> u64 {
        match request {
            InfiniteLoopRequest::Query => {
                let mut n: usize = 10;
                loop {
                    for _ in 0..n {
                        for _ in 0..n {
                            let mut sum = 0;
                            for i in 0..n {
                                sum += i;
                            }
                            let sum_anal = n * (n-1) / 2;
                            if sum != sum_anal {
                                return n as u64;
                            }
                        }
                    }
                    n += 1;
                }
            },
        }
    }
}
