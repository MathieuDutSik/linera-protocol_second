// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_sdk::views::{linera_views, RegisterView, RootView, ViewStorageContext};

/// The application state.
#[derive(RootView)]
#[view(context = ViewStorageContext)]
pub struct IntensiveCpuState {
    pub value: RegisterView<u64>,
}

impl IntensiveCpuState {
    fn single_increment(v: &mut Vec<u8>, n: u64) -> bool {
        for i in 0..n {
            if v[i as usize] == 0 {
                v[i as usize]=1;
                for j in 0..i {
                    v[j as usize] = 0;
                }
                return false;
            }
        }
        true
    }

    pub(crate) fn power_test(n: u64) {
        assert!(n < 63);
        let mut v = Vec::new();
        let mut analytic_value = 1 as u64;
        for _ in 0..n {
            v.push(0);
            analytic_value *= 2;
        }
        let mut n_iter = 0 as u64;
        loop {
            n_iter += 1;
            let test = Self::single_increment(&mut v, n);
            if test {
                break;
            }
        }
        assert_eq!(n_iter, analytic_value);
    }
}
