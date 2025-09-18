#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use reported_solutions::{ReportedSolutionsAbi, ReportedSolutionsOperation};
use linera_sdk::{
    linera_base_types::WithContractAbi,
    views::{RootView, View},
    Contract, ContractRuntime,
};

use self::state::ReportedSolutionsState;

pub struct ReportedSolutionsContract {
    state: ReportedSolutionsState,
    runtime: ContractRuntime<Self>,
}

linera_sdk::contract!(ReportedSolutionsContract);

impl WithContractAbi for ReportedSolutionsContract {
    type Abi = ReportedSolutionsAbi;
}

impl Contract for ReportedSolutionsContract {
    type Message = ();
    type InstantiationArgument = ();
    type Parameters = ();
    type EventValue = ();

    async fn load(runtime: ContractRuntime<Self>) -> Self {
        let state = ReportedSolutionsState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        ReportedSolutionsContract { state, runtime }
    }

    async fn instantiate(&mut self, _argument: ()) {
        // Validate that the application parameters were configured correctly.
        self.runtime.application_parameters();
    }

    async fn execute_operation(&mut self, operation: ReportedSolutionsOperation) {
        match operation {
            ReportedSolutionsOperation::SetSolution { key1, key2, value } => {
                let inner_collection = self.state.reported_solutions.load_entry_mut(&key1).await
                    .expect("Failed to load inner collection");
                let register = inner_collection.load_entry_mut(&key2).await
                    .expect("Failed to load register view");
                register.set(value);
            }
            ReportedSolutionsOperation::RemoveInnerEntry { key1, key2 } => {
                let inner_collection = self.state.reported_solutions.load_entry_mut(&key1).await
                    .expect("Failed to load inner collection");
                inner_collection.remove_entry(&key2)
                    .expect("Failed to remove inner entry");
            }
            ReportedSolutionsOperation::RemoveOuterEntry { key1 } => {
                self.state.reported_solutions.remove_entry(&key1)
                    .expect("Failed to remove outer entry");
            }
        }
    }

    async fn execute_message(&mut self, _message: ()) {
        panic!("ReportedSolutions application doesn't support any cross-chain messages");
    }

    async fn store(mut self) {
        self.state.save().await.expect("Failed to save state");
    }
}