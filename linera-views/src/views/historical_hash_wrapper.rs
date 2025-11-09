// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::ops::{Deref, DerefMut};

#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use serde::{Deserialize, Serialize};

use crate::{
    batch::Batch,
    common::from_bytes_option,
    context::Context,
    register_view::RegisterView,
    views::{ClonableView, HashableView, Hasher, HasherOutput, ReplaceContext, View, ViewError, MIN_VIEW_TAG},
};

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{exponential_bucket_latencies, register_histogram_vec};
    use prometheus::HistogramVec;

    /// The runtime of hash computation
    pub static HISTORICALLY_HASHABLE_VIEW_HASH_RUNTIME: LazyLock<HistogramVec> =
        LazyLock::new(|| {
            register_histogram_vec(
                "historically_hashable_view_hash_runtime",
                "HistoricallyHashableView hash runtime",
                &[],
                exponential_bucket_latencies(5.0),
            )
        });
}

/// Wrapper to compute the hash of the view based on its history of modifications.
#[derive(Debug)]
pub struct HistoricallyHashableView<C, W> {
    /// The historical hash in storage.
    stored_historical_hash: Option<HasherOutput>,
    /// Memoized historical hash, if any.
    historical_hash: Option<HasherOutput>,
    /// The state hash in storage.
    stored_state_hash: Option<HasherOutput>,
    /// Memoized state hash, if any.
    state_hash: Option<HasherOutput>,
    /// The inner view.
    inner: W,
    /// The hash choice option
    choice: RegisterView<C, HashChoice>,
}

/// The possible choice for the hashing method.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub enum HashChoice {
    /// The historical hash computed from the batch.
    HistoricalHash,
    /// The hash computed from the state of the view.
    #[default]
    StateHash,
}

/// Key tags to create the sub-keys of a `HistoricallyHashableView` on top of the base key.
#[repr(u8)]
enum KeyTag {
    /// Prefix for the indices of the view.
    Inner = MIN_VIEW_TAG,
    /// Prefix for the hash choice.
    Choice,
    /// Prefix for the historical hash.
    HistoricalHash,
    /// Prefix for the state hash.
    StateHash,
}

impl<C, W> HistoricallyHashableView<C, W> {
    fn make_historical_hash(
        stored_historical_hash: Option<HasherOutput>,
        batch: &Batch,
    ) -> Result<HasherOutput, ViewError> {
        #[cfg(with_metrics)]
        let _hash_latency = metrics::HISTORICALLY_HASHABLE_VIEW_HASH_RUNTIME.measure_latency();
        let stored_historical_hash = stored_historical_hash.unwrap_or_default();
        if batch.is_empty() {
            return Ok(stored_historical_hash);
        }
        let mut hasher = sha3::Sha3_256::default();
        hasher.update_with_bytes(&stored_historical_hash)?;
        hasher.update_with_bcs_bytes(&batch)?;
        Ok(hasher.finalize())
    }
}

impl<C, W, C2> ReplaceContext<C2> for HistoricallyHashableView<C, W>
where
    W: View<Context = C> + ReplaceContext<C2>,
    C: Context,
    C2: Context,
{
    type Target = HistoricallyHashableView<C2, <W as ReplaceContext<C2>>::Target>;

    async fn with_context(
        &mut self,
        ctx: impl FnOnce(&Self::Context) -> C2 + Clone,
    ) -> Self::Target {
        HistoricallyHashableView {
            stored_historical_hash: self.stored_historical_hash,
            historical_hash: self.historical_hash,
            stored_state_hash: self.stored_state_hash,
            state_hash: self.state_hash,
            inner: self.inner.with_context(ctx.clone()).await,
            choice: self.choice.with_context(ctx).await,
        }
    }
}

impl<W> View for HistoricallyHashableView<W::Context, W>
where
    W: View,
{
    const NUM_INIT_KEYS: usize = 3 + W::NUM_INIT_KEYS;

    type Context = W::Context;

    fn context(&self) -> &Self::Context {
        self.inner.context()
    }

    fn pre_load(context: &Self::Context) -> Result<Vec<Vec<u8>>, ViewError> {
        let mut v = vec![context.base_key().base_tag(KeyTag::HistoricalHash as u8),
                         context.base_key().base_tag(KeyTag::StateHash as u8),
                         context.base_key().base_tag(KeyTag::Choice as u8),
        ];
        let base_key = context.base_key().base_tag(KeyTag::Inner as u8);
        let context = context.clone_with_base_key(base_key);
        v.extend(W::pre_load(&context)?);
        Ok(v)
    }

    fn post_load(context: Self::Context, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let historical_hash = from_bytes_option(values.first().ok_or(ViewError::PostLoadValuesError)?)?;
        let state_hash = from_bytes_option(values.get(1).ok_or(ViewError::PostLoadValuesError)?)?;
        let base_key = context.base_key().base_tag(KeyTag::Choice as u8);
        let context_choice = context.clone_with_base_key(base_key);
        let choice = RegisterView::post_load(
            context_choice,
            values.get(2..3).ok_or(ViewError::PostLoadValuesError)?,
        )?;
        let base_key = context.base_key().base_tag(KeyTag::Inner as u8);
        let context_inner = context.clone_with_base_key(base_key);
        let inner = W::post_load(
            context_inner,
            values.get(2..).ok_or(ViewError::PostLoadValuesError)?,
        )?;
        Ok(Self {
            stored_historical_hash: historical_hash,
            historical_hash,
            stored_state_hash: state_hash,
            state_hash,
            inner,
            choice,
        })
    }

    fn rollback(&mut self) {
        self.inner.rollback();
        self.historical_hash = self.stored_historical_hash;
        self.state_hash = self.stored_historical_hash;
    }

    async fn has_pending_changes(&self) -> bool {
        self.inner.has_pending_changes().await ||
            self.choice.has_pending_changes().await ||
            self.stored_state_hash != self.state_hash
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        // Computes the inner_batch
        let mut inner_batch = Batch::new();
        let delete_view = self.inner.flush(&mut inner_batch)?;
        if delete_view {
            // Deleteting the view completely.
            let mut key_prefix = self.inner.context().base_key().bytes.clone();
            key_prefix.pop();
            batch.delete_key_prefix(key_prefix);
            self.stored_historical_hash = None;
            self.historical_hash = None;
            self.stored_state_hash = None;
            self.state_hash = None;
        } else {
            // Computes the historical hash even if not asked
            let historical_hash = Self::make_historical_hash(self.stored_historical_hash, &inner_batch)?;
            batch.operations.extend(inner_batch.operations);
            if self.stored_historical_hash != Some(historical_hash) {
                let mut key = self.inner.context().base_key().bytes.clone();
                let tag = key.last_mut().unwrap();
                *tag = KeyTag::HistoricalHash as u8;
                batch.put_key_value(key, &historical_hash)?;
                self.stored_historical_hash = Some(historical_hash);
            }
            self.historical_hash = Some(historical_hash);
            // Writes the state hash if needed.
            if self.stored_state_hash != self.state_hash {
                let mut key = self.inner.context().base_key().bytes.clone();
                let tag = key.last_mut().unwrap();
                *tag = KeyTag::StateHash as u8;
                match &self.state_hash {
                    None => batch.delete_key(key),
                    Some(hash) => batch.put_key_value(key, hash)?,
                }
                self.stored_state_hash = self.state_hash;
            }
        }
        // Returning whether the view is deleted or not.
        Ok(delete_view)
    }

    fn clear(&mut self) {
        self.inner.clear();
        self.historical_hash = None;
        self.state_hash = None;
    }
}

impl<W> ClonableView for HistoricallyHashableView<W::Context, W>
where
    W: ClonableView,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(HistoricallyHashableView {
            stored_historical_hash: self.stored_historical_hash,
            historical_hash: self.historical_hash,
            stored_state_hash: self.stored_state_hash,
            state_hash: self.state_hash,
            inner: self.inner.clone_unchecked()?,
            choice: self.choice.clone_unchecked()?,
        })
    }
}

impl<W: ClonableView> HistoricallyHashableView<W::Context, W> {
    /// Obtains a hash of the history of the changes in the view.
    pub async fn historical_hash(&mut self) -> Result<HasherOutput, ViewError> {
        if let Some(historical_hash) = self.historical_hash {
            return Ok(historical_hash);
        }
        let mut batch = Batch::new();
        if self.inner.has_pending_changes().await {
            let mut inner = self.inner.clone_unchecked()?;
            inner.flush(&mut batch)?;
        }
        let historical_hash = Self::make_historical_hash(self.stored_historical_hash, &batch)?;
        // Remember the hash that we just computed.
        self.historical_hash = Some(historical_hash);
        Ok(historical_hash)
    }
}




impl<W> HistoricallyHashableView<W::Context, W>
where
    W: HashableView,
    W::Hasher: Hasher<Output = HasherOutput>,
{
    /// Returns the state hash of the view.
    pub async fn state_hash(&mut self) -> Result<HasherOutput, ViewError> {
        match self.state_hash {
            Some(state_hash) => Ok(state_hash),
            None => {
                let new_state_hash = self.inner.hash_mut().await?;
                self.state_hash = Some(new_state_hash);
                Ok(new_state_hash)
            }
        }
    }
}

impl<W> HistoricallyHashableView<W::Context, W>
where
    W: HashableView + ClonableView,
    W::Hasher: Hasher<Output = HasherOutput>,
{
    /// Returns the historical or state hash of the view.
    pub async fn historical_or_state_hash(&mut self) -> Result<HasherOutput, ViewError> {
        let choice = self.choice.get();
        match choice {
            HashChoice::HistoricalHash => self.historical_hash().await,
            HashChoice::StateHash => self.state_hash().await,
        }
    }
}









impl<C, W> Deref for HistoricallyHashableView<C, W> {
    type Target = W;

    fn deref(&self) -> &W {
        &self.inner
    }
}

impl<C, W> DerefMut for HistoricallyHashableView<C, W> {
    fn deref_mut(&mut self) -> &mut W {
        // Clear the memoized hash.
        self.historical_hash = None;
        self.state_hash = None;
        &mut self.inner
    }
}

#[cfg(with_graphql)]
mod graphql {
    use std::borrow::Cow;

    use super::HistoricallyHashableView;
    use crate::context::Context;

    impl<C, W> async_graphql::OutputType for HistoricallyHashableView<C, W>
    where
        C: Context,
        W: async_graphql::OutputType + Send + Sync,
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
            self.inner.resolve(ctx, field).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        context::MemoryContext, register_view::RegisterView, store::WritableKeyValueStore as _,
    };

    #[tokio::test]
    async fn test_historically_hashable_view_initial_state() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut view =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;

        // Initially should have no pending changes
        assert!(!view.has_pending_changes().await);

        // Initial hash should be the hash of an empty batch with default stored_hash
        let hash = view.historical_hash().await?;
        assert_eq!(hash, HasherOutput::default());

        Ok(())
    }

    #[tokio::test]
    async fn test_historically_hashable_view_hash_changes_with_modifications(
    ) -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut view =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;

        // Get initial hash
        let hash0 = view.historical_hash().await?;

        // Set a value
        view.set(42);
        assert!(view.has_pending_changes().await);

        // Hash should change after modification
        let hash1 = view.historical_hash().await?;

        // Calling `historical_hash` doesn't flush changes.
        assert!(view.has_pending_changes().await);
        assert_ne!(hash0, hash1);

        // Flush and verify hash is stored
        let mut batch = Batch::new();
        view.flush(&mut batch)?;
        context.store().write_batch(batch).await?;
        assert!(!view.has_pending_changes().await);
        assert_eq!(hash1, view.historical_hash().await?);

        // Make another modification
        view.set(84);
        let hash2 = view.historical_hash().await?;
        assert_ne!(hash1, hash2);

        Ok(())
    }

    #[tokio::test]
    async fn test_historically_hashable_view_reloaded() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut view =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;

        // Set initial value and flush
        view.set(42);
        let mut batch = Batch::new();
        view.flush(&mut batch)?;
        context.store().write_batch(batch).await?;

        let hash_after_flush = view.historical_hash().await?;

        // Reload the view
        let mut view2 =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;

        // Hash should be the same (loaded from storage)
        let hash_reloaded = view2.historical_hash().await?;
        assert_eq!(hash_after_flush, hash_reloaded);

        Ok(())
    }

    #[tokio::test]
    async fn test_historically_hashable_view_rollback() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut view =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;

        // Set and persist a value
        view.set(42);
        let mut batch = Batch::new();
        view.flush(&mut batch)?;
        context.store().write_batch(batch).await?;

        let hash_before = view.historical_hash().await?;
        assert!(!view.has_pending_changes().await);

        // Make a modification
        view.set(84);
        assert!(view.has_pending_changes().await);
        let hash_modified = view.historical_hash().await?;
        assert_ne!(hash_before, hash_modified);

        // Rollback
        view.rollback();
        assert!(!view.has_pending_changes().await);

        // Hash should return to previous value
        let hash_after_rollback = view.historical_hash().await?;
        assert_eq!(hash_before, hash_after_rollback);

        Ok(())
    }

    #[tokio::test]
    async fn test_historically_hashable_view_clear() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut view =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;

        // Set and persist a value
        view.set(42);
        let mut batch = Batch::new();
        view.flush(&mut batch)?;
        context.store().write_batch(batch).await?;

        assert_ne!(view.historical_hash().await?, HasherOutput::default());

        // Clear the view
        view.clear();
        assert!(view.has_pending_changes().await);

        // Flush the clear operation
        let mut batch = Batch::new();
        let delete_view = view.flush(&mut batch)?;
        assert!(!delete_view);
        context.store().write_batch(batch).await?;

        // Verify the view is not reset to default
        assert_ne!(view.historical_hash().await?, HasherOutput::default());

        Ok(())
    }

    #[tokio::test]
    async fn test_historically_hashable_view_clone_unchecked() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut view =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;

        // Set a value
        view.set(42);
        let mut batch = Batch::new();
        view.flush(&mut batch)?;
        context.store().write_batch(batch).await?;

        let original_hash = view.historical_hash().await?;

        // Clone the view
        let mut cloned_view = view.clone_unchecked()?;

        // Verify the clone has the same hash initially
        let cloned_hash = cloned_view.historical_hash().await?;
        assert_eq!(original_hash, cloned_hash);

        // Modify the clone
        cloned_view.set(84);
        let cloned_hash_after = cloned_view.historical_hash().await?;
        assert_ne!(original_hash, cloned_hash_after);

        // Original should be unchanged
        let original_hash_after = view.historical_hash().await?;
        assert_eq!(original_hash, original_hash_after);

        Ok(())
    }

    #[tokio::test]
    async fn test_historically_hashable_view_flush_updates_stored_hash() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut view =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;

        // Initial state - no stored hash
        assert!(!view.has_pending_changes().await);

        // Set a value
        view.set(42);
        assert!(view.has_pending_changes().await);

        let hash_before_flush = view.historical_hash().await?;

        // Flush - this should update stored_hash
        let mut batch = Batch::new();
        let delete_view = view.flush(&mut batch)?;
        assert!(!delete_view);
        context.store().write_batch(batch).await?;

        assert!(!view.has_pending_changes().await);

        // Make another change
        view.set(84);
        let hash_after_second_change = view.historical_hash().await?;

        // The new hash should be based on the previous stored hash
        assert_ne!(hash_before_flush, hash_after_second_change);

        Ok(())
    }

    #[tokio::test]
    async fn test_historically_hashable_view_deref() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut view =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;

        // Test Deref - we can access inner view methods directly
        view.set(42);
        assert_eq!(*view.get(), 42);

        // Test DerefMut
        view.set(84);
        assert_eq!(*view.get(), 84);

        Ok(())
    }

    #[tokio::test]
    async fn test_historically_hashable_view_sequential_modifications() -> Result<(), ViewError> {
        async fn get_hash(values: &[u32]) -> Result<HasherOutput, ViewError> {
            let context = MemoryContext::new_for_testing(());
            let mut view =
                HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;

            let mut previous_hash = view.historical_hash().await?;
            for &value in values {
                view.set(value);
                if value % 2 == 0 {
                    // Immediately save after odd values.
                    let mut batch = Batch::new();
                    view.flush(&mut batch)?;
                    context.store().write_batch(batch).await?;
                }
                let current_hash = view.historical_hash().await?;
                assert_ne!(previous_hash, current_hash);
                previous_hash = current_hash;
            }
            Ok(previous_hash)
        }

        let h1 = get_hash(&[10, 20, 30, 40, 50]).await?;
        let h2 = get_hash(&[20, 30, 40, 50]).await?;
        let h3 = get_hash(&[20, 21, 30, 40, 50]).await?;
        assert_ne!(h1, h2);
        assert_eq!(h2, h3);
        Ok(())
    }

    #[tokio::test]
    async fn test_historically_hashable_view_flush_with_no_hash_change() -> Result<(), ViewError> {
        let context = MemoryContext::new_for_testing(());
        let mut view =
            HistoricallyHashableView::<_, RegisterView<_, u32>>::load(context.clone()).await?;

        // Set and flush a value
        view.set(42);
        let mut batch = Batch::new();
        view.flush(&mut batch)?;
        context.store().write_batch(batch).await?;

        let hash_before = view.historical_hash().await?;

        // Flush again without changes - no new hash should be stored
        let mut batch = Batch::new();
        view.flush(&mut batch)?;
        assert!(batch.is_empty());
        context.store().write_batch(batch).await?;

        let hash_after = view.historical_hash().await?;
        assert_eq!(hash_before, hash_after);

        Ok(())
    }
}
