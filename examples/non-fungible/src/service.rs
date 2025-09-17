// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(target_arch = "wasm32", no_main)]

mod state;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use log::info;
use async_graphql::{EmptySubscription, Object, Request, Response, Schema};
use base64::engine::{general_purpose::STANDARD_NO_PAD, Engine as _};
use linera_sdk::{
    linera_base_types::{Account, AccountOwner, DataBlobHash, WithServiceAbi},
    views::View,
    Service, ServiceRuntime,
};
use non_fungible::{NftOutput, Operation, TokenId};

use self::state::NonFungibleTokenState;

pub struct NonFungibleTokenService {
    state: Arc<NonFungibleTokenState>,
    runtime: Arc<ServiceRuntime<Self>>,
}

linera_sdk::service!(NonFungibleTokenService);

impl WithServiceAbi for NonFungibleTokenService {
    type Abi = non_fungible::NonFungibleTokenAbi;
}

impl Service for NonFungibleTokenService {
    type Parameters = ();

    async fn new(runtime: ServiceRuntime<Self>) -> Self {
        let state = NonFungibleTokenState::load(runtime.root_view_storage_context())
            .await
            .expect("Failed to load state");
        NonFungibleTokenService {
            state: Arc::new(state),
            runtime: Arc::new(runtime),
        }
    }

    async fn handle_query(&self, request: Request) -> Response {
        let schema = Schema::build(
            QueryRoot {
                non_fungible_token: self.state.clone(),
                runtime: self.runtime.clone(),
            },
            MutationRoot {
                runtime: self.runtime.clone(),
            },
            EmptySubscription,
        )
        .finish();
        schema.execute(request).await
    }
}

struct QueryRoot {
    non_fungible_token: Arc<NonFungibleTokenState>,
    runtime: Arc<ServiceRuntime<NonFungibleTokenService>>,
}

#[Object]
impl QueryRoot {
    async fn nft(&self, token_id: String) -> Option<NftOutput> {
        info!("QueryRoot::nft, token_id={}", token_id);
        let token_id_vec = STANDARD_NO_PAD.decode(&token_id).unwrap();
        let count = self.non_fungible_token.nfts.count().await.expect("service::count");
        info!("QueryRoot::nft, count={count}");
        let nft = self
            .non_fungible_token
            .nfts
            .get(&TokenId { id: token_id_vec })
            .await
            .unwrap();
        info!("QueryRoot::nft, nft={:?}", nft);

        if let Some(nft) = nft {
            info!("QueryRoot::nft, Before read_data_blob hash={:?}", nft.blob_hash);
            let payload = self.runtime.read_data_blob(nft.blob_hash);
            info!("QueryRoot::nft, After read_data_blob, payload={payload:?}");
            let nft_output = NftOutput::new_with_token_id(token_id, nft, payload);
            Some(nft_output)
        } else {
            info!("QueryRoot::nft, returning None");
            None
        }
    }

    async fn nftfail(&self, _token_id: String) -> Option<NftOutput> {
        use linera_sdk::linera_base_types::CryptoHash;
        info!("QueryRoot::nft_fail, Before read_data_blob");
        let mut vec = [0_u8; 32];
        for i in 0..32 {
            vec[i] = i as u8;
        }
        let hash: CryptoHash = CryptoHash::from(vec);
        let blob_hash = DataBlobHash(hash);
        let _payload = self.runtime.read_data_blob(blob_hash);
        info!("QueryRoot::nft_fail, After read_data_blob");
        None
    }

    async fn nfts(&self) -> BTreeMap<String, NftOutput> {
        let mut nfts = BTreeMap::new();
        self.non_fungible_token
            .nfts
            .for_each_index_value(|_token_id, nft| {
                let nft = nft.into_owned();
                let payload = self.runtime.read_data_blob(nft.blob_hash);
                let nft_output = NftOutput::new(nft, payload);
                nfts.insert(nft_output.token_id.clone(), nft_output);
                Ok(())
            })
            .await
            .unwrap();

        nfts
    }

    async fn owned_token_ids_by_owner(&self, owner: AccountOwner) -> BTreeSet<String> {
        self.non_fungible_token
            .owned_token_ids
            .get(&owner)
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .map(|token_id| STANDARD_NO_PAD.encode(token_id.id))
            .collect()
    }

    async fn owned_token_ids(&self) -> BTreeMap<AccountOwner, BTreeSet<String>> {
        let mut owners = BTreeMap::new();
        self.non_fungible_token
            .owned_token_ids
            .for_each_index_value(|owner, token_ids| {
                let token_ids = token_ids.into_owned();
                let new_token_ids = token_ids
                    .into_iter()
                    .map(|token_id| STANDARD_NO_PAD.encode(token_id.id))
                    .collect();

                owners.insert(owner, new_token_ids);
                Ok(())
            })
            .await
            .unwrap();

        owners
    }

    async fn owned_nfts(&self, owner: AccountOwner) -> BTreeMap<String, NftOutput> {
        let mut result = BTreeMap::new();
        let owned_token_ids = self
            .non_fungible_token
            .owned_token_ids
            .get(&owner)
            .await
            .unwrap();

        for token_id in owned_token_ids.into_iter().flatten() {
            let nft = self
                .non_fungible_token
                .nfts
                .get(&token_id)
                .await
                .unwrap()
                .unwrap();
            let payload = self.runtime.read_data_blob(nft.blob_hash);
            let nft_output = NftOutput::new(nft, payload);
            result.insert(nft_output.token_id.clone(), nft_output);
        }

        result
    }
}

struct MutationRoot {
    runtime: Arc<ServiceRuntime<NonFungibleTokenService>>,
}

#[Object]
impl MutationRoot {
    async fn mint(&self, minter: AccountOwner, name: String, blob_hash: DataBlobHash) -> [u8; 0] {
        let operation = Operation::Mint {
            minter,
            name,
            blob_hash,
        };
        self.runtime.schedule_operation(&operation);
        []
    }

    async fn transfer(
        &self,
        source_owner: AccountOwner,
        token_id: String,
        target_account: Account,
    ) -> [u8; 0] {
        let operation = Operation::Transfer {
            source_owner,
            token_id: TokenId {
                id: STANDARD_NO_PAD.decode(token_id).unwrap(),
            },
            target_account,
        };
        self.runtime.schedule_operation(&operation);
        []
    }

    async fn claim(
        &self,
        source_account: Account,
        token_id: String,
        target_account: Account,
    ) -> [u8; 0] {
        let operation = Operation::Claim {
            source_account,
            token_id: TokenId {
                id: STANDARD_NO_PAD.decode(token_id).unwrap(),
            },
            target_account,
        };
        self.runtime.schedule_operation(&operation);
        []
    }
}
