// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// The historical hash is computed from the batch

use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};
use linera_base::ensure;

use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::Batch,
    common::from_bytes_option,
    context::Context,
    store::ReadableKeyValueStore as _,
    views::{ClonableView, HashableView, Hasher, ReplaceContext, View, ViewError, MIN_VIEW_TAG},
};

/// A hash for `ContainerView` and storing of the hash for memoization purposes
#[derive(Debug)]
pub struct HistoricalHashContainerView<C, W, O> {
    _phantom: PhantomData<C>,
    stored_hash: O,
    inner: W,
}

/// Key tags to create the sub-keys of a `MapView` on top of the base key.
#[repr(u8)]
enum KeyTag {
    /// Prefix for the indices of the view.
    Inner = MIN_VIEW_TAG,
    /// Prefix for the hash.
    Hash,
}

fn get_default_hash<H: Hasher>() -> H::Output {
    let hasher = H::default();
    hasher.finalize()
}


impl<C, W, O, C2> ReplaceContext<C2> for HistoricalHashContainerView<C, W, O>
where
    W: HashableView<Hasher: Hasher<Output = O>, Context = C> + ReplaceContext<C2>,
    <W as ReplaceContext<C2>>::Target: HashableView<Hasher: Hasher<Output = O>>,
    O: Serialize + DeserializeOwned + Send + Sync + Copy + PartialEq,
    C: Context,
    C2: Context,
{
    type Target = HistoricalHashContainerView<C2, <W as ReplaceContext<C2>>::Target, O>;

    async fn with_context(
        &mut self,
        ctx: impl FnOnce(&Self::Context) -> C2 + Clone,
    ) -> Self::Target {
        HistoricalHashContainerView {
            _phantom: PhantomData,
            stored_hash: self.stored_hash,
            inner: self.inner.with_context(ctx).await,
        }
    }
}

impl<W, O> View for HistoricalHashContainerView<W::Context, W, O>
where
    W: HashableView<Hasher: Hasher<Output = O>>,
    O: Serialize + DeserializeOwned + Send + Sync + Copy + PartialEq,
{
    const NUM_INIT_KEYS: usize = 1 + W::NUM_INIT_KEYS;

    type Context = W::Context;

    fn context(&self) -> &Self::Context {
        self.inner.context()
    }

    fn pre_load(context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError> {
        let mut v = vec![context.base_key().base_tag(KeyTag::Hash as u8)];
        let base_key = context.base_key().base_tag(KeyTag::Inner as u8);
        let context = context.clone_with_base_key(base_key);
        v.extend(W::pre_load(&context)?);
        Ok(v)
    }

    fn post_load(context: Self::Context, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let hash = from_bytes_option(values.first().ok_or(ViewError::PostLoadValuesError)?)?;
        let stored_hash = match hash {
            None => get_default_hash::<W::Hasher>(),
            Some(hash) => hash,
        };
        let base_key = context.base_key().base_tag(KeyTag::Inner as u8);
        let context = context.clone_with_base_key(base_key);
        let inner = W::post_load(
            context,
            values.get(1..).ok_or(ViewError::PostLoadValuesError)?,
        )?;
        Ok(Self {
            _phantom: PhantomData,
            stored_hash,
            inner,
        })
    }

    async fn load(context: Self::Context) -> Result<Self, ViewError> {
        let keys = Self::pre_load(&context)?;
        let values = context.store().read_multi_values_bytes(keys).await?;
        Self::post_load(context, &values)
    }

    fn rollback(&mut self) {
        self.inner.rollback();
    }

    async fn has_pending_changes(&self) -> bool {
        self.inner.has_pending_changes().await
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        let delete_view = self.inner.flush(batch)?;
        if delete_view {
            // delete the view and revert the stored_hash to the initial one.
            let mut key_prefix = self.inner.context().base_key().bytes.clone();
            key_prefix.pop();
            batch.delete_key_prefix(key_prefix);
            self.stored_hash = get_default_hash::<W::Hasher>();
        } else {
            self.stored_hash = batch.compute_incremental_hash::<W::Hasher>(self.stored_hash)?;
            let mut key = self.inner.context().base_key().bytes.clone();
            let tag = key.last_mut().unwrap();
            *tag = KeyTag::Hash as u8;
            batch.put_key_value(key, &self.stored_hash)?;
        }
        Ok(delete_view)
    }

    fn clear(&mut self) {
        self.inner.clear();
    }
}

impl<W, O> ClonableView for HistoricalHashContainerView<W::Context, W, O>
where
    W: HashableView + ClonableView,
    O: Serialize + DeserializeOwned + Send + Sync + Copy + PartialEq,
    W::Hasher: Hasher<Output = O>,
{
    fn clone_unchecked(&mut self) -> Self {
        HistoricalHashContainerView {
            _phantom: PhantomData,
            stored_hash: self.stored_hash,
            inner: self.inner.clone_unchecked(),
        }
    }
}

impl<W, O> HashableView for HistoricalHashContainerView<W::Context, W, O>
where
    W: HashableView<Hasher: Hasher<Output = O>>,
    O: Serialize + DeserializeOwned + Send + Sync + Copy + PartialEq,
{
    type Hasher = W::Hasher;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.hash().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        ensure!(!self.has_pending_changes().await, ViewError::CannotProvideHistoricalHash);
        Ok(self.stored_hash)
    }
}

impl<C, W, O> Deref for HistoricalHashContainerView<C, W, O> {
    type Target = W;

    fn deref(&self) -> &W {
        &self.inner
    }
}

impl<C, W, O> DerefMut for HistoricalHashContainerView<C, W, O> {
    fn deref_mut(&mut self) -> &mut W {
        &mut self.inner
    }
}

#[cfg(with_graphql)]
mod graphql {
    use std::borrow::Cow;

    use super::HistoricalHashContainerView;
    use crate::context::Context;

    impl<C, W, O> async_graphql::OutputType for HistoricalHashContainerView<C, W, O>
    where
        C: Context,
        W: async_graphql::OutputType + Send + Sync,
        O: Send + Sync,
    {
        fn type_name() -> Cow<'static, str> {
            W::type_name()
        }

        fn qualified_type_name() -> String {
            W::qualified_type_name()
        }

        fn create_type_info(registry: &mut async_graphql::registry::Registry) -> String {
            W::create_type_info(registry)
        }

        async fn resolve(
            &self,
            ctx: &async_graphql::ContextSelectionSet<'_>,
            field: &async_graphql::Positioned<async_graphql::parser::types::Field>,
        ) -> async_graphql::ServerResult<async_graphql::Value> {
            (**self).resolve(ctx, field).await
        }
    }
}
