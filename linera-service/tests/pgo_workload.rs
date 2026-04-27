// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A comprehensive PGO (Profile-Guided Optimization) workload test.
//!
//! This test exercises social, fungible (allowances + native-fungible), non-fungible,
//! crowd-funding, matching-engine, and AMM applications on a single-validator,
//! single-shard local network to generate representative profiling data.
//!
//! Run with:
//!   cargo test -p linera-service --features storage-service pgo_workload -- --nocapture

#![cfg(feature = "storage-service")]

mod guard;

use std::collections::BTreeMap;

use anyhow::Result;
use async_graphql::InputType;
use guard::INTEGRATION_TEST_GUARD;
use linera_base::{
    crypto::CryptoHash,
    data_types::{Amount, Timestamp},
    identifiers::{Account, AccountOwner, ApplicationId},
    vm::VmRuntime,
};
use linera_sdk::{
    abis::fungible::FungibleTokenAbi,
    linera_base_types::{BlobContent, DataBlobHash},
};
use linera_service::{
    cli_wrappers::{
        local_net::{get_node_port, Database, LocalNetConfig, ProcessInbox},
        ApplicationWrapper, ClientWrapper, LineraNet, LineraNetConfig, Network,
    },
    test_name,
};
use serde_json::Value;

fn get_account_owner(client: &ClientWrapper) -> AccountOwner {
    client.get_owner().unwrap()
}

// ---------------------------------------------------------------------------
// Helper wrappers (duplicated from linera_net_tests.rs to keep this test
// self-contained — these are thin GraphQL wrappers, not business logic).
// ---------------------------------------------------------------------------

#[allow(dead_code)]
struct FungibleApp(ApplicationWrapper<fungible::FungibleTokenAbi>);

#[allow(dead_code)]
impl FungibleApp {
    async fn get_amount(&self, account_owner: &AccountOwner) -> Amount {
        let query = format!(
            "accounts {{ entry(key: {}) {{ value }} }}",
            account_owner.to_value()
        );
        let response_body = self.0.query(&query).await.unwrap();
        let amount_option = serde_json::from_value::<Option<Amount>>(
            response_body["accounts"]["entry"]["value"].clone(),
        )
        .unwrap();
        amount_option.unwrap_or(Amount::ZERO)
    }

    async fn assert_balances(&self, accounts: impl IntoIterator<Item = (AccountOwner, Amount)>) {
        for (account_owner, amount) in accounts {
            let value = self.get_amount(&account_owner).await;
            assert_eq!(value, amount);
        }
    }

    async fn transfer(
        &self,
        account_owner: &AccountOwner,
        amount_transfer: Amount,
        destination: Account,
    ) -> Value {
        let mutation = format!(
            "transfer(owner: {}, amount: \"{}\", targetAccount: {})",
            account_owner.to_value(),
            amount_transfer,
            destination.to_value(),
        );
        self.0.mutate(mutation).await.unwrap()
    }

    async fn approve(
        &self,
        owner: &AccountOwner,
        spender: &AccountOwner,
        allowance: Amount,
    ) -> Value {
        let mutation = format!(
            "approve(owner: {}, spender: {}, allowance: \"{}\")",
            owner.to_value(),
            spender.to_value(),
            allowance,
        );
        self.0.mutate(mutation).await.unwrap()
    }

    async fn transfer_from(
        &self,
        owner: &AccountOwner,
        spender: &AccountOwner,
        amount_transfer: Amount,
        destination: Account,
    ) -> Value {
        let mutation = format!(
            "transferFrom(owner: {}, spender: {}, amount: \"{}\", targetAccount: {})",
            owner.to_value(),
            spender.to_value(),
            amount_transfer,
            destination.to_value(),
        );
        self.0.mutate(mutation).await.unwrap()
    }
}

#[allow(dead_code)]
struct NonFungibleApp(ApplicationWrapper<non_fungible::NonFungibleTokenAbi>);

#[allow(dead_code)]
impl NonFungibleApp {
    fn create_token_id(
        chain_id: &linera_base::identifiers::ChainId,
        application_id: &linera_base::identifiers::ApplicationId,
        name: &String,
        minter: &AccountOwner,
        hash: &DataBlobHash,
        num_minted_nfts: u64,
    ) -> String {
        use base64::engine::{general_purpose::STANDARD_NO_PAD, Engine as _};
        let token_id_vec = non_fungible::Nft::create_token_id(
            chain_id,
            application_id,
            name,
            minter,
            hash,
            num_minted_nfts,
        )
        .expect("Creating token ID should not fail");
        STANDARD_NO_PAD.encode(token_id_vec.id)
    }

    async fn get_nft(&self, token_id: &String) -> Result<non_fungible::NftOutput> {
        let query = format!(
            "nft(tokenId: {}) {{ tokenId, owner, name, minter, payload }}",
            token_id.to_value()
        );
        let response_body = self.0.query(&query).await?;
        Ok(serde_json::from_value(response_body["nft"].clone())?)
    }

    async fn get_owned_nfts(&self, owner: &AccountOwner) -> Result<Vec<String>> {
        let query = format!("ownedTokenIdsByOwner(owner: {})", owner.to_value());
        let response_body = self.0.query(&query).await?;
        Ok(serde_json::from_value(
            response_body["ownedTokenIdsByOwner"].clone(),
        )?)
    }

    async fn mint(&self, minter: &AccountOwner, name: &String, blob_hash: &DataBlobHash) -> Value {
        let mutation = format!(
            "mint(minter: {}, name: {}, blobHash: {})",
            minter.to_value(),
            name.to_value(),
            blob_hash.to_value(),
        );
        self.0.mutate(mutation).await.unwrap()
    }

    async fn transfer(
        &self,
        source_owner: &AccountOwner,
        token_id: &String,
        target_account: &Account,
    ) -> Value {
        let mutation = format!(
            "transfer(sourceOwner: {}, tokenId: {}, targetAccount: {})",
            source_owner.to_value(),
            token_id.to_value(),
            target_account.to_value(),
        );
        self.0.mutate(mutation).await.unwrap()
    }
}

struct MatchingEngineApp(ApplicationWrapper<matching_engine::MatchingEngineAbi>);

impl MatchingEngineApp {
    async fn get_account_info(
        &self,
        account_owner: &AccountOwner,
    ) -> Vec<matching_engine::OrderId> {
        let query = format!(
            "accountInfo {{ entry(key: {}) {{ value {{ orders }} }} }}",
            account_owner.to_value()
        );
        let response_body = self.0.query(query).await.unwrap();
        let orders_value = &response_body["accountInfo"]["entry"]["value"]["orders"];
        if orders_value.is_null() {
            return Vec::new();
        }
        serde_json::from_value(orders_value.clone()).unwrap()
    }

    async fn order(&self, order: matching_engine::Order) -> Value {
        let mutation = format!("executeOrder(order: {})", order.to_value());
        self.0.mutate(mutation).await.unwrap()
    }
}

struct AmmApp(ApplicationWrapper<amm::AmmAbi>);

impl AmmApp {
    async fn swap(
        &self,
        owner: AccountOwner,
        input_token_idx: u32,
        input_amount: Amount,
    ) -> Result<Value> {
        let mutation = format!(
            "swap(owner: {}, inputTokenIdx: {}, inputAmount: \"{}\")",
            owner.to_value(),
            input_token_idx,
            input_amount
        );
        self.0.mutate(mutation).await
    }

    async fn add_liquidity(
        &self,
        owner: AccountOwner,
        max_token0_amount: Amount,
        max_token1_amount: Amount,
    ) -> Result<Value> {
        let mutation = format!(
            "addLiquidity(owner: {}, maxToken0Amount: \"{}\", maxToken1Amount: \"{}\")",
            owner.to_value(),
            max_token0_amount,
            max_token1_amount
        );
        self.0.mutate(mutation).await
    }

    async fn remove_liquidity(
        &self,
        owner: AccountOwner,
        token_to_remove_idx: u32,
        token_to_remove_amount: Amount,
    ) -> Result<Value> {
        let mutation = format!(
            "removeLiquidity(owner: {}, tokenToRemoveIdx: {}, tokenToRemoveAmount: \"{}\")",
            owner.to_value(),
            token_to_remove_idx,
            token_to_remove_amount
        );
        self.0.mutate(mutation).await
    }
}

async fn publish_and_create_fungible(
    client: &ClientWrapper,
    name: &str,
    params: &fungible::Parameters,
    state: &fungible::InitialState,
    chain_id: Option<linera_base::identifiers::ChainId>,
) -> Result<ApplicationId<FungibleTokenAbi>> {
    let (contract, service) = client.build_example(name).await?;
    if name == "native-fungible" {
        client
            .publish_and_create::<FungibleTokenAbi, fungible::Parameters, fungible::InitialState>(
                contract,
                service,
                VmRuntime::Wasm,
                params,
                state,
                &[],
                chain_id,
            )
            .await
    } else {
        let application_id = client
            .publish_and_create::<FungibleTokenAbi, fungible::Parameters, fungible::InitialState>(
                contract,
                service,
                VmRuntime::Wasm,
                params,
                state,
                &[],
                chain_id,
            )
            .await?;
        Ok(application_id.forget_abi().with_abi())
    }
}

/// Single-validator, single-shard config for PGO profiling.
fn pgo_config() -> LocalNetConfig {
    let mut config = LocalNetConfig::new_test(Database::Service, Network::Grpc);
    config.num_initial_validators = 1;
    config.num_shards = 1;
    config
}

// =========================================================================
// The main PGO workload test
// =========================================================================

#[test_log::test(tokio::test)]
async fn test_pgo_workload() -> Result<()> {
    let _guard = INTEGRATION_TEST_GUARD.lock().await;
    tracing::info!("Starting PGO workload test {}", test_name!());

    let config = pgo_config();
    let (mut net, client_admin) = config.instantiate().await?;

    let client1 = net.make_client().await;
    client1.wallet_init(None).await?;
    let client2 = net.make_client().await;
    client2.wallet_init(None).await?;

    let chain_admin = client_admin.load_wallet()?.default_chain().unwrap();
    let chain1 = client_admin
        .open_and_assign(&client1, Amount::from_tokens(1000))
        .await?;
    let chain2 = client_admin
        .open_and_assign(&client2, Amount::from_tokens(1000))
        .await?;

    let owner_admin = get_account_owner(&client_admin);
    let owner1 = get_account_owner(&client1);
    let owner2 = get_account_owner(&client2);

    // =================================================================
    // Pre-compile all wasm examples and deploy apps that use the CLI
    // (client.publish_and_create) BEFORE starting node services,
    // because the node service holds the RocksDB lock.
    // =================================================================
    tracing::info!("=== PGO: Building wasm examples ===");

    // 1. Social
    use social::SocialAbi;
    let (social_contract, social_service) = client_admin.build_example("social").await?;
    let social_module_id = client_admin
        .publish_module::<SocialAbi, (), ()>(
            social_contract, social_service, VmRuntime::Wasm, None,
        )
        .await?;
    let social_app_id = client_admin
        .create_application(&social_module_id, &(), &(), &[], None)
        .await?;

    // 2. Fungible (for allowances test)
    // Tokens deployed on chain_admin, so owner_admin must hold them (only chain owner can sign).
    let fungible_accounts = BTreeMap::from([
        (owner_admin, Amount::from_tokens(150)),
    ]);
    let fungible_state = fungible::InitialState { accounts: fungible_accounts };
    let fungible_params = fungible::Parameters::new("FUN");
    let fungible_app_id =
        publish_and_create_fungible(&client_admin, "fungible", &fungible_params, &fungible_state, None).await?;

    // 3. Native fungible
    let native_fungible_state = fungible::InitialState {
        accounts: BTreeMap::from([
            (owner_admin, Amount::from_tokens(80)),
        ]),
    };
    let native_fungible_params = fungible::Parameters::new("NAT");
    let native_fungible_app_id = publish_and_create_fungible(
        &client_admin,
        "native-fungible",
        &native_fungible_params,
        &native_fungible_state,
        None,
    ).await?;

    // 4. Non-fungible
    use non_fungible::NonFungibleTokenAbi;
    let (nft_contract, nft_service) = client_admin.build_example("non-fungible").await?;
    let nft_app_id = client_admin
        .publish_and_create::<NonFungibleTokenAbi, (), ()>(
            nft_contract, nft_service, VmRuntime::Wasm, &(), &(), &[], None,
        )
        .await?;

    // 5. Crowd-funding: fungible token + crowd-funding app
    use crowd_funding::{CrowdFundingAbi, InstantiationArgument};
    let cf_state = fungible::InitialState {
        accounts: BTreeMap::from([(owner_admin, Amount::from_tokens(100))]),
    };
    let cf_params = fungible::Parameters::new("CFT");
    let (cf_contract_f, cf_service_f) = client_admin.build_example("fungible").await?;
    let cf_fungible_id = client_admin
        .publish_and_create::<FungibleTokenAbi, fungible::Parameters, fungible::InitialState>(
            cf_contract_f, cf_service_f, VmRuntime::Wasm,
            &cf_params, &cf_state, &[], None,
        )
        .await?;
    let cf_crowd_state = InstantiationArgument {
        owner: owner_admin,
        deadline: Timestamp::from(u64::MAX),
        target: Amount::from_tokens(10),
    };
    let (cf_contract_crowd, cf_service_crowd) = client_admin.build_example("crowd-funding").await?;
    let crowd_app_id = client_admin
        .publish_and_create::<
            CrowdFundingAbi,
            ApplicationId<FungibleTokenAbi>,
            InstantiationArgument,
        >(
            cf_contract_crowd, cf_service_crowd, VmRuntime::Wasm,
            &cf_fungible_id, &cf_crowd_state, &[cf_fungible_id.forget_abi()], None,
        )
        .await?;

    // 6. Matching engine: two fungible tokens on client1/client2 + engine on admin
    use matching_engine::{MatchingEngineAbi, OrderNature, Price};
    let me_state_a = fungible::InitialState {
        accounts: BTreeMap::from([(owner1, Amount::from_tokens(1000))]),
    };
    let (me_contract_fa, me_service_fa) = client1.build_example("fungible").await?;
    let me_token_a = client1
        .publish_and_create::<FungibleTokenAbi, fungible::Parameters, fungible::InitialState>(
            me_contract_fa, me_service_fa, VmRuntime::Wasm,
            &fungible::Parameters::new("MEA"), &me_state_a, &[], None,
        )
        .await?;
    let me_state_b = fungible::InitialState {
        accounts: BTreeMap::from([(owner2, Amount::from_tokens(1000))]),
    };
    let (me_contract_fb, me_service_fb) = client2.build_example("fungible").await?;
    let me_token_b = client2
        .publish_and_create::<FungibleTokenAbi, fungible::Parameters, fungible::InitialState>(
            me_contract_fb, me_service_fb, VmRuntime::Wasm,
            &fungible::Parameters::new("MEB"), &me_state_b, &[], None,
        )
        .await?;
    let (me_contract, me_service) = client_admin.build_example("matching-engine").await?;

    // 7. AMM: pre-build wasm only (deploy via node service since it needs the tokens)
    use amm::{AmmAbi, Parameters as AmmParameters};
    let (amm_contract_fungible, amm_service_fungible) = client_admin.build_example("fungible").await?;
    let (amm_contract, amm_service) = client_admin.build_example("amm").await?;

    // ----- Start node services -----
    tracing::info!("=== PGO: Starting node services ===");
    let port_admin = get_node_port().await;
    let port1 = get_node_port().await;
    let port2 = get_node_port().await;
    let mut ns_admin = client_admin
        .run_node_service(port_admin, ProcessInbox::Automatic)
        .await?;
    let mut ns1 = client1
        .run_node_service(port1, ProcessInbox::Automatic)
        .await?;
    let mut ns2 = client2
        .run_node_service(port2, ProcessInbox::Automatic)
        .await?;

    // =====================================================================
    // 1. SOCIAL — 50 posts
    // =====================================================================
    tracing::info!("=== PGO: Social (50 posts) ===");
    {
        let app_social_admin =
            ns_admin.make_application(&chain_admin, &social_app_id)?;
        let app_social1 = ns1.make_application(&chain1, &social_app_id)?;

        // client1 subscribes to admin's posts
        app_social1
            .mutate(format!("subscribe(chainId: \"{}\")", chain_admin))
            .await?;

        // Admin creates 50 posts
        for i in 0..50 {
            app_social_admin
                .mutate(format!("post(text: \"PGO post number {}\")", i))
                .await?;
        }

        // Give some time for messages to propagate with automatic inbox processing
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        // Verify that client1 received the posts
        let query = "receivedPosts { keys { author, index } }";
        let response = app_social1.query(query).await?;
        let keys = response["receivedPosts"]["keys"]
            .as_array()
            .expect("keys should be an array");
        tracing::info!("Social: client1 received {} posts", keys.len());
        assert!(
            keys.len() >= 1,
            "client1 should have received at least some posts"
        );
    }

    // =====================================================================
    // 2. ALLOWANCES FUNGIBLE (fungible + native-fungible)
    // =====================================================================
    tracing::info!("=== PGO: Allowances fungible ===");
    {
        let app_f_admin =
            FungibleApp(ns_admin.make_application(&chain_admin, &fungible_app_id)?);

        // Transfers from owner_admin (chain owner) to other chains
        for i in 0..10 {
            app_f_admin
                .transfer(
                    &owner_admin,
                    Amount::from_tokens(1),
                    Account {
                        chain_id: chain1,
                        owner: owner1,
                    },
                )
                .await;
            tracing::info!("Fungible transfer {} done", i);
        }

        // Allowances: owner_admin approves owner1 to spend
        app_f_admin
            .approve(&owner_admin, &owner1, Amount::from_tokens(20))
            .await;
        app_f_admin
            .approve(&owner_admin, &owner1, Amount::from_tokens(10))
            .await;

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let app_nf_admin =
            FungibleApp(ns_admin.make_application(&chain_admin, &native_fungible_app_id)?);

        // More transfers with native fungible from owner_admin
        for i in 0..10 {
            app_nf_admin
                .transfer(
                    &owner_admin,
                    Amount::from_tokens(1),
                    Account {
                        chain_id: chain2,
                        owner: owner2,
                    },
                )
                .await;
            tracing::info!("Native fungible transfer {} done", i);
        }

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }

    // =====================================================================
    // 3. NON-FUNGIBLE
    // =====================================================================
    tracing::info!("=== PGO: Non-fungible ===");
    {
        let app_nft_admin =
            NonFungibleApp(ns_admin.make_application(&chain_admin, &nft_app_id)?);

        // Mint several NFTs and transfer them around
        for i in 0..10 {
            let nft_name = format!("pgo_nft_{}", i);
            let nft_blob_bytes = format!("pgo_nft_data_{}", i).into_bytes();
            let nft_blob_hash = CryptoHash::new(&BlobContent::new_data(nft_blob_bytes.clone()));
            ns_admin
                .publish_data_blob(&chain_admin, nft_blob_bytes)
                .await?;

            let nft_blob_hash = DataBlobHash(nft_blob_hash);

            let token_id = NonFungibleApp::create_token_id(
                &chain_admin,
                &nft_app_id.forget_abi(),
                &nft_name,
                &owner_admin,
                &nft_blob_hash,
                i as u64,
            );

            app_nft_admin.mint(&owner_admin, &nft_name, &nft_blob_hash).await;

            // Transfer even-numbered NFTs to chain1
            if i % 2 == 0 {
                app_nft_admin
                    .transfer(
                        &owner_admin,
                        &token_id,
                        &Account {
                            chain_id: chain1,
                            owner: owner1,
                        },
                    )
                    .await;
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }

    // =====================================================================
    // 4. CROWD-FUNDING (with fungible)
    // =====================================================================
    tracing::info!("=== PGO: Crowd-funding ===");
    {
        let app_cf_fungible =
            FungibleApp(ns_admin.make_application(&chain_admin, &cf_fungible_id)?);

        // Transfer some tokens to client2 for pledging
        app_cf_fungible
            .transfer(
                &owner_admin,
                Amount::from_tokens(30),
                Account {
                    chain_id: chain2,
                    owner: owner2,
                },
            )
            .await;

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let app_crowd2 = ns2.make_application(&chain2, &crowd_app_id)?;

        // Multiple pledges
        for i in 0..5 {
            let mutation = format!(
                "pledge(owner: {}, amount: \"{}\")",
                owner2.to_value(),
                Amount::from_tokens(2),
            );
            app_crowd2.mutate(mutation).await?;
            tracing::info!("Crowd-funding pledge {} done", i);
        }

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        // Collect
        let app_crowd_admin = ns_admin.make_application(&chain_admin, &crowd_app_id)?;
        app_crowd_admin.mutate("collect").await?;
    }

    // =====================================================================
    // 5. MATCHING ENGINE — 50 bids + 50 asks
    // =====================================================================
    tracing::info!("=== PGO: Matching engine (50 bids + 50 asks) ===");
    {
        // Deploy matching engine on admin chain via node service
        let me_params = matching_engine::Parameters {
            tokens: [me_token_a, me_token_b],
            price_decimals: 0,
        };
        let me_module_id = ns_admin
            .publish_module::<MatchingEngineAbi, matching_engine::Parameters, ()>(
                &chain_admin,
                me_contract,
                me_service,
                VmRuntime::Wasm,
            )
            .await?;
        let me_app_id = ns_admin
            .create_application(
                &chain_admin,
                &me_module_id,
                &me_params,
                &(),
                &[me_token_a.forget_abi(), me_token_b.forget_abi()],
            )
            .await?;

        let app_me1 =
            MatchingEngineApp(ns1.make_application(&chain1, &me_app_id)?);
        let app_me2 =
            MatchingEngineApp(ns2.make_application(&chain2, &me_app_id)?);

        // Owner1 creates 50 bids at varying prices
        for i in 0..50 {
            let price = (i % 10) + 1; // prices 1-10
            app_me1
                .order(matching_engine::Order::Insert {
                    owner: owner1,
                    quantity: Amount::from_tokens(1),
                    nature: OrderNature::Bid,
                    price: Price { price },
                })
                .await;
            if i % 10 == 9 {
                tracing::info!("Matching engine: {} bids placed", i + 1);
            }
        }

        // Owner2 creates 50 asks at varying prices
        for i in 0..50 {
            let price = (i % 10) + 1; // prices 1-10
            app_me2
                .order(matching_engine::Order::Insert {
                    owner: owner2,
                    quantity: Amount::from_tokens(1),
                    nature: OrderNature::Ask,
                    price: Price { price },
                })
                .await;
            if i % 10 == 9 {
                tracing::info!("Matching engine: {} asks placed", i + 1);
            }
        }

        // Wait for processing
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        // Read account info to exercise query paths
        let orders_a = app_me1.get_account_info(&owner1).await;
        let orders_b = app_me2.get_account_info(&owner2).await;
        tracing::info!(
            "Matching engine: owner1 has {} remaining orders, owner2 has {} remaining orders",
            orders_a.len(),
            orders_b.len()
        );

        // Cancel remaining orders
        let app_me_admin =
            MatchingEngineApp(ns_admin.make_application(&chain_admin, &me_app_id)?);
        let orders_a = app_me_admin.get_account_info(&owner1).await;
        let orders_b = app_me_admin.get_account_info(&owner2).await;
        for order_id in orders_a {
            app_me1
                .order(matching_engine::Order::Cancel {
                    owner: owner1,
                    order_id,
                })
                .await;
        }
        for order_id in orders_b {
            app_me2
                .order(matching_engine::Order::Cancel {
                    owner: owner2,
                    order_id,
                })
                .await;
        }

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }

    // =====================================================================
    // 6. AMM
    // =====================================================================
    tracing::info!("=== PGO: AMM ===");
    {
        // Create two fungible tokens on admin chain via node service
        let fungible_module_id = ns_admin
            .publish_module::<FungibleTokenAbi, fungible::Parameters, fungible::InitialState>(
                &chain_admin,
                amm_contract_fungible,
                amm_service_fungible,
                VmRuntime::Wasm,
            )
            .await?;

        let state0 = fungible::InitialState {
            accounts: BTreeMap::from([(owner_admin, Amount::from_tokens(500))]),
        };
        let params0 = fungible::Parameters::new("AMZ");
        let amm_token0 = ns_admin
            .create_application(&chain_admin, &fungible_module_id, &params0, &state0, &[])
            .await?;

        let state1 = fungible::InitialState {
            accounts: BTreeMap::from([(owner_admin, Amount::from_tokens(500))]),
        };
        let params1 = fungible::Parameters::new("AMO");
        let amm_token1 = ns_admin
            .create_application(&chain_admin, &fungible_module_id, &params1, &state1, &[])
            .await?;

        let app_ft0_admin =
            FungibleApp(ns_admin.make_application(&chain_admin, &amm_token0)?);
        let app_ft1_admin =
            FungibleApp(ns_admin.make_application(&chain_admin, &amm_token1)?);

        // Transfer tokens to chain1 for owner1 to add liquidity and swap
        app_ft0_admin
            .transfer(
                &owner_admin,
                Amount::from_tokens(200),
                Account {
                    chain_id: chain1,
                    owner: owner1,
                },
            )
            .await;
        app_ft1_admin
            .transfer(
                &owner_admin,
                Amount::from_tokens(200),
                Account {
                    chain_id: chain1,
                    owner: owner1,
                },
            )
            .await;
        // Transfer tokens to chain2 for owner2
        app_ft0_admin
            .transfer(
                &owner_admin,
                Amount::from_tokens(200),
                Account {
                    chain_id: chain2,
                    owner: owner2,
                },
            )
            .await;
        app_ft1_admin
            .transfer(
                &owner_admin,
                Amount::from_tokens(200),
                Account {
                    chain_id: chain2,
                    owner: owner2,
                },
            )
            .await;

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        // Deploy AMM via node service
        let amm_params = AmmParameters {
            tokens: [amm_token0, amm_token1],
        };
        let amm_module_id = ns_admin
            .publish_module::<AmmAbi, AmmParameters, ()>(
                &chain_admin,
                amm_contract,
                amm_service,
                VmRuntime::Wasm,
            )
            .await?;
        let amm_app_id = ns_admin
            .create_application(
                &chain_admin,
                &amm_module_id,
                &amm_params,
                &(),
                &[amm_token0.forget_abi(), amm_token1.forget_abi()],
            )
            .await?;

        let app_amm1 = AmmApp(ns1.make_application(&chain1, &amm_app_id)?);
        let app_amm2 = AmmApp(ns2.make_application(&chain2, &amm_app_id)?);

        // Add liquidity from owner1
        app_amm1
            .add_liquidity(owner1, Amount::from_tokens(100), Amount::from_tokens(100))
            .await?;

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        // Add liquidity from owner2
        app_amm2
            .add_liquidity(owner2, Amount::from_tokens(100), Amount::from_tokens(100))
            .await?;

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        // Perform several swaps
        for i in 0..5 {
            app_amm1
                .swap(owner1, 0, Amount::from_tokens(5))
                .await?;
            tracing::info!("AMM swap {} (owner1, token0->token1) done", i);
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        for i in 0..5 {
            app_amm2
                .swap(owner2, 1, Amount::from_tokens(5))
                .await?;
            tracing::info!("AMM swap {} (owner2, token1->token0) done", i);
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }

        // Remove some liquidity
        app_amm1
            .remove_liquidity(owner1, 0, Amount::from_tokens(10))
            .await?;

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }

    // =====================================================================
    // Cleanup
    // =====================================================================
    tracing::info!("=== PGO workload complete ===");

    ns_admin.ensure_is_running()?;
    ns1.ensure_is_running()?;
    ns2.ensure_is_running()?;

    net.ensure_is_running().await?;
    net.terminate().await?;

    Ok(())
}
