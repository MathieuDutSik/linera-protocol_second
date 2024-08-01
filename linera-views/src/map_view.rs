// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The `MapView` implements a map that can be modified.
//!
//! This reproduces more or less the functionalities of the `BTreeMap`.
//! There are 3 different variants:
//! * The [`ByteMapView`][class1] whose keys are the `Vec<u8>` and the values are a serializable type `V`.
//!   The ordering of the entries is via the lexicographic order of the keys.
//! * The [`MapView`][class2] whose keys are a serializable type `K` and the value a serializable type `V`.
//!   The ordering is via the order of the BCS serialized keys.
//! * The [`CustomMapView`][class3] whose keys are a serializable type `K` and the value a serializable type `V`.
//!   The ordering is via the order of the custom serialized keys.
//!
//! [class1]: map_view::ByteMapView
//! [class2]: map_view::MapView
//! [class3]: map_view::CustomMapView

#[cfg(with_metrics)]
use std::sync::LazyLock;

#[cfg(with_metrics)]
use {
    linera_base::prometheus_util::{self, MeasureLatency},
    prometheus::HistogramVec,
};

#[repr(u8)]
enum KeyTag {
    /// Prefix for the storing of the small map
    SmallMap = MIN_VIEW_TAG,
    /// Prefix for the indices of the map
    Index,
}

#[cfg(with_metrics)]
/// The runtime of hash computation
static MAP_VIEW_HASH_RUNTIME: LazyLock<HistogramVec> = LazyLock::new(|| {
    prometheus_util::register_histogram_vec(
        "map_view_hash_runtime",
        "MapView hash runtime",
        &[],
        Some(vec![
            0.001, 0.003, 0.01, 0.03, 0.1, 0.2, 0.3, 0.4, 0.5, 0.75, 1.0, 2.0, 5.0,
        ]),
    )
    .expect("Histogram can be created")
});

/// The threshold for choosing between small maps stored as a single value
const LIMIT_SMALL_MAP: usize = 1000;

use std::{
    borrow::Borrow,
    collections::{btree_map::Entry, BTreeMap},
    fmt::Debug,
    marker::PhantomData,
    mem,
};

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    batch::Batch,
    common::{
        from_bytes_option, get_interval, Context, CustomSerialize, DeletionSet, HasherOutput, KeyIterable,
        KeyValueIterable, MIN_VIEW_TAG, SuffixClosedSetIterator, Update,
    },
    hashable_wrapper::WrappedHashableContainerView,
    views::{ClonableView, HashableView, Hasher, View, ViewError},
};

#[derive(Debug, Clone)]
enum MapState<V> {
    BigMap {
        updates: BTreeMap<Vec<u8>, Update<V>>,
        deletion_set: DeletionSet,
    },
    SmallMap {
	small_map: BTreeMap<Vec<u8>, V>,
        stored_small_map: BTreeMap<Vec<u8>, V>,
        delete_storage_first: bool,
    },
}

/// A view that supports inserting and removing values indexed by `Vec<u8>`.
#[derive(Debug)]
pub struct ByteMapView<C, V> {
    context: C,
    map_state: MapState<V>,
}

#[async_trait]
impl<C, V> View<C> for ByteMapView<C, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    V: Send + Sync + Serialize + DeserializeOwned + Clone,
{
    const NUM_INIT_KEYS: usize = 0;

    fn context(&self) -> &C {
        &self.context
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        Ok(vec![context.base_tag(KeyTag::SmallMap as u8)])
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let value = values.first().ok_or(ViewError::PostLoadValuesError)?;
        let value = from_bytes_option::<Option<BTreeMap<Vec<u8>, V>>, _>(value)?;
        let map_state = match value {
            None => {
                // No existing data, start with a small map
                MapState::SmallMap { small_map: BTreeMap::new(), stored_small_map: BTreeMap::new(), delete_storage_first: false }
            }
            Some(option) => {
                match option {
                    None => MapState::BigMap { updates: BTreeMap::new(), deletion_set: DeletionSet::new() },
                    Some(map) => {
                        MapState::SmallMap { small_map: map.clone(), stored_small_map: map, delete_storage_first: false }
                    }
                }
            }
        };
        Ok(Self { context, map_state })
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        let keys = Self::pre_load(&context)?;
        let values = context.read_multi_values_bytes(keys).await?;
        Self::post_load(context, &values)
    }

    fn rollback(&mut self) {
        match &mut self.map_state {
            MapState::BigMap { updates, deletion_set } => {
                updates.clear();
                deletion_set.rollback();
            },
            MapState::SmallMap { small_map, stored_small_map, delete_storage_first } => {
                *small_map = stored_small_map.clone();
                *delete_storage_first = false;
            }
        }
    }

    async fn has_pending_changes(&self) -> bool {
        match &self.map_state {
            MapState::BigMap { updates, deletion_set } => {
                deletion_set.has_pending_changes() || !updates.is_empty()
            },
            MapState::SmallMap { small_map, delete_storage_first, .. } => {
                *delete_storage_first || small_map.len() > 0
            },
        }
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        let mut delete_view = false;
        match &mut self.map_state {
            MapState::BigMap { updates, deletion_set } => {
                if deletion_set.delete_storage_first {
                    delete_view = true;
                    batch.delete_key_prefix(self.context.base_key());
                    for (index, update) in mem::take(updates) {
                        if let Update::Set(value) = update {
                            let key = self.context.base_tag_index(KeyTag::Index as u8, &index);
                            batch.put_key_value(key, &value)?;
                            delete_view = false;
                        }
                    }
                } else {
                    for index in mem::take(&mut deletion_set.deleted_prefixes) {
                        let key = self.context.base_tag_index(KeyTag::Index as u8, &index);
                        batch.delete_key_prefix(key);
                    }
                    for (index, update) in mem::take(updates) {
                        let key = self.context.base_tag_index(KeyTag::Index as u8, &index);
                        match update {
                            Update::Removed => batch.delete_key(key),
                            Update::Set(value) => batch.put_key_value(key, &value)?,
                        }
                    }
                }
                deletion_set.delete_storage_first = false;
            },
            MapState::SmallMap { small_map, stored_small_map, delete_storage_first } => {
                if *delete_storage_first {
                    batch.delete_key(self.context.base_key());
                    delete_view = true;
                }
                if small_map.len() > 0 {
                    let key = self.context.base_tag(KeyTag::SmallMap as u8);
                    batch.put_key_value(key, &Some(small_map.clone()))?;
                    *stored_small_map = small_map.clone();
                }
                *delete_storage_first = false;
            },
        }
        Ok(delete_view)
    }

    fn clear(&mut self) {
        self.map_state = MapState::SmallMap { small_map: BTreeMap::new(), stored_small_map: BTreeMap::new(), delete_storage_first: true };
    }
}

impl<C, V> ClonableView<C> for ByteMapView<C, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    V: Clone + Send + Sync + Serialize + DeserializeOwned,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(ByteMapView {
            context: self.context.clone(),
            map_state: self.map_state.clone(),
        })
    }
}

impl<C, V> ByteMapView<C, V>
where
    C: Context,
    ViewError: From<C::Error>,
    V: Clone,
{
    /// Inserts or resets the value of a key of the map.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], String::from("Hello"));
    ///   assert_eq!(map.keys().await.unwrap(), vec![vec![0,1]]);
    /// # })
    /// ```
    pub fn insert(&mut self, short_key: Vec<u8>, value: V) {
        let crit_size = match &mut self.map_state {
            MapState::BigMap { updates, .. } => {
                updates.insert(short_key, Update::Set(value));
                0
            }
            MapState::SmallMap { small_map, .. } => {
                small_map.insert(short_key, value);
                small_map.len()
            },
        };
        if crit_size >= LIMIT_SMALL_MAP {
            let MapState::SmallMap { small_map, .. } = &self.map_state else {
                unreachable!()
            };
            let updates = small_map.into_iter().map(|(key,value)| {
                let value: V = value.clone();
                (key.clone(), Update::Set(value))
            })
                .collect::<BTreeMap<Vec<u8>,Update<V>>>();
            self.map_state = MapState::BigMap { updates, deletion_set: DeletionSet::new() };
        }
    }

    /// Removes a value. If absent then nothing is done.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], "Hello");
    ///   map.remove(vec![0,1]);
    /// # })
    /// ```
    pub fn remove(&mut self, short_key: Vec<u8>) {
        match &mut self.map_state {
            MapState::BigMap { updates, deletion_set } => {
                if deletion_set.contains_prefix_of(&short_key) {
                    // Optimization: No need to mark `short_key` for deletion as we are going to remove all the keys at once.
                    updates.remove(&short_key);
                } else {
                    updates.insert(short_key, Update::Removed);
                }
            },
            MapState::SmallMap { small_map, .. } => {
                small_map.remove(&short_key);
            },
        }
    }

    /// Removes a value. If absent then nothing is done.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], String::from("Hello"));
    ///   map.insert(vec![0,2], String::from("Bonjour"));
    ///   map.remove_by_prefix(vec![0]);
    ///   assert!(map.keys().await.unwrap().is_empty());
    /// # })
    /// ```
    pub fn remove_by_prefix(&mut self, key_prefix: Vec<u8>) {
        match &mut self.map_state {
            MapState::BigMap { updates, deletion_set } => {
                let key_list = updates
                    .range(get_interval(key_prefix.clone()))
                    .map(|x| x.0.to_vec())
                    .collect::<Vec<_>>();
                for key in key_list {
                    updates.remove(&key);
                }
                deletion_set.insert_key_prefix(key_prefix);
            },
            MapState::SmallMap { small_map, .. } => {
                let key_list = small_map
                    .range(get_interval(key_prefix.clone()))
                    .map(|x| x.0.to_vec())
                    .collect::<Vec<_>>();
                for key in key_list {
                    small_map.remove(&key);
                }
            },
        }
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.context.extra()
    }

    /// Returns `true` if the map contains a value for the specified key.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0, 1], String::from("Hello"));
    ///   assert!(map.contains_key(&[0,1]).await.unwrap());
    ///   assert!(!map.contains_key(&[0,2]).await.unwrap());
    /// # })
    /// ```
    pub async fn contains_key(&self, short_key: &[u8]) -> Result<bool, ViewError> {
        match &self.map_state {
            MapState::BigMap { updates, deletion_set } => {
                if let Some(update) = updates.get(short_key) {
                    let test = match update {
                        Update::Removed => false,
                        Update::Set(_value) => true,
                    };
                    return Ok(test);
                }
                if deletion_set.contains_prefix_of(short_key) {
                    return Ok(false);
                }
                let key = self.context.base_tag_index(KeyTag::Index as u8, short_key);
                Ok(self.context.contains_key(&key).await?)
            },
            MapState::SmallMap { small_map, .. } => {
                Ok(small_map.contains_key(short_key))
            },
        }
    }
}

impl<C, V> ByteMapView<C, V>
where
    C: Context + Sync,
    ViewError: From<C::Error>,
    V: Clone + DeserializeOwned + 'static,
{
    /// Reads the value at the given position, if any.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], String::from("Hello"));
    ///   assert_eq!(map.get(&[0,1]).await.unwrap(), Some(String::from("Hello")));
    /// # })
    /// ```
    pub async fn get(&self, short_key: &[u8]) -> Result<Option<V>, ViewError> {
        match &self.map_state {
            MapState::BigMap { updates, deletion_set } => {
                if let Some(update) = updates.get(short_key) {
                    let value = match update {
                        Update::Removed => None,
                        Update::Set(value) => Some(value.clone()),
                    };
                    return Ok(value);
                }
                if deletion_set.contains_prefix_of(short_key) {
                    return Ok(None);
                }
                let key = self.context.base_tag_index(KeyTag::Index as u8, short_key);
                Ok(self.context.read_value(&key).await?)
            },
            MapState::SmallMap { small_map, .. } => {
                Ok(small_map.get(short_key).cloned())
            },
        }
    }

    /// Obtains a mutable reference to a value at a given position if available.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], String::from("Hello"));
    ///   let value = map.get_mut(&[0,1]).await.unwrap().unwrap();
    ///   assert_eq!(*value, String::from("Hello"));
    ///   *value = String::from("Hola");
    ///   assert_eq!(map.get(&[0,1]).await.unwrap(), Some(String::from("Hola")));
    /// # })
    /// ```
    pub async fn get_mut(&mut self, short_key: &[u8]) -> Result<Option<&mut V>, ViewError> {
        match &mut self.map_state {
            MapState::BigMap { updates, deletion_set } => {
                let update = match updates.entry(short_key.to_vec()) {
                    Entry::Vacant(e) => {
                        if deletion_set.contains_prefix_of(short_key) {
                            None
                        } else {
                            let key = self.context.base_tag_index(KeyTag::Index as u8, short_key);
                            let value = self.context.read_value(&key).await?;
                            value.map(|x| e.insert(Update::Set(x)))
                        }
                    }
                    Entry::Occupied(e) => {
                        let e = e.into_mut();
                        Some(e)
                    }
                };
                Ok(match update {
                    Some(Update::Set(value)) => Some(value),
                    _ => None,
                })
            },
            MapState::SmallMap { small_map, .. } => {
                Ok(small_map.get_mut(short_key))
            },
        }
    }
}

impl<C, V> ByteMapView<C, V>
where
    C: Context,
    ViewError: From<C::Error>,
    V: Sync + Serialize + DeserializeOwned + 'static,
{
    /// The big map case
    async fn for_each_key_while_big<F>(context: &C, updates: &BTreeMap<Vec<u8>,Update<V>>, deletion_set: &DeletionSet, mut f: F, prefix: Vec<u8>) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<bool, ViewError> + Send,
    {
        let prefix_len = prefix.len();
        let mut updates = updates.range(get_interval(prefix.clone()));
        let mut update = updates.next();
        if !deletion_set.contains_prefix_of(&prefix) {
            let iter = deletion_set
                .deleted_prefixes
                .range(get_interval(prefix.clone()));
            let mut suffix_closed_set = SuffixClosedSetIterator::new(prefix_len, iter);
            let base = context.base_index(&prefix);
            for index in context.find_keys_by_prefix(&base).await?.iterator() {
                let index = index?;
                loop {
                    match update {
                        Some((key, value)) if &key[prefix_len..] <= index => {
                            if let Update::Set(_) = value {
                                if !f(&key[prefix_len..])? {
                                    return Ok(());
                                }
                            }
                            update = updates.next();
                            if &key[prefix_len..] == index {
                                break;
                            }
                        }
                        _ => {
                            if !suffix_closed_set.find_key(index) && !f(index)? {
                                return Ok(());
                            }
                            break;
                        }
                    }
                }
            }
        }
        while let Some((key, value)) = update {
            if let Update::Set(_) = value {
                if !f(&key[prefix_len..])? {
                    return Ok(());
                }
            }
            update = updates.next();
        }
        Ok(())
    }

    /// Applies the function f on each index (aka key) which has the assigned prefix.
    /// Keys are visited in the lexicographic order. The shortened key is send to the
    /// function and if it returns false, then the loop exits
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], String::from("Hello"));
    ///   map.insert(vec![1,2], String::from("Bonjour"));
    ///   map.insert(vec![1,3], String::from("Bonjour"));
    ///   let prefix = vec![1];
    ///   let mut count = 0;
    ///   map.for_each_key_while(|_key| {
    ///     count += 1;
    ///     Ok(count < 3)
    ///   }, prefix).await.unwrap();
    ///   assert_eq!(count, 2);
    /// # })
    /// ```
    pub async fn for_each_key_while<F>(&self, mut f: F, prefix: Vec<u8>) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<bool, ViewError> + Send,
    {
        match &self.map_state {
            MapState::BigMap { updates, deletion_set } => {
                Self::for_each_key_while_big(&self.context, &updates, &deletion_set, f, prefix).await
            },
            MapState::SmallMap { small_map, .. } => {
		for (key, _) in small_map {
                    if !f(key)? {
                        return Ok(());
                    }
                }
                Ok(())
            },
        }
    }

    /// Applies the function f on each index (aka key) having the specified prefix.
    /// The shortened keys are sent to the function f. Keys are visited in the
    /// lexicographic order.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], String::from("Hello"));
    ///   let mut count = 0;
    ///   let prefix = Vec::new();
    ///   map.for_each_key(|_key| {
    ///     count += 1;
    ///     Ok(())
    ///   }, prefix).await.unwrap();
    ///   assert_eq!(count, 1);
    /// # })
    /// ```
    pub async fn for_each_key<F>(&self, mut f: F, prefix: Vec<u8>) -> Result<(), ViewError>
    where
        F: FnMut(&[u8]) -> Result<(), ViewError> + Send,
    {
        self.for_each_key_while(
            |key| {
                f(key)?;
                Ok(true)
            },
            prefix,
        )
        .await
    }

    /// Returns the list of keys of the map in lexicographic order.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], String::from("Hello"));
    ///   map.insert(vec![1,2], String::from("Bonjour"));
    ///   map.insert(vec![2,2], String::from("Hallo"));
    ///   assert_eq!(map.keys().await.unwrap(), vec![vec![0,1], vec![1,2], vec![2,2]]);
    /// # })
    /// ```
    pub async fn keys(&self) -> Result<Vec<Vec<u8>>, ViewError> {
        let mut keys = Vec::new();
        let prefix = Vec::new();
        self.for_each_key(
            |key| {
                keys.push(key.to_vec());
                Ok(())
            },
            prefix,
        )
        .await?;
        Ok(keys)
    }

    /// Returns the list of keys of the map having a specified prefix
    /// in lexicographic order.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], String::from("Hello"));
    ///   map.insert(vec![1,2], String::from("Bonjour"));
    ///   map.insert(vec![1,3], String::from("Hallo"));
    ///   assert_eq!(map.keys_by_prefix(vec![1]).await.unwrap(), vec![vec![1,2], vec![1,3]]);
    /// # })
    /// ```
    pub async fn keys_by_prefix(&self, prefix: Vec<u8>) -> Result<Vec<Vec<u8>>, ViewError> {
        let mut keys = Vec::new();
        let prefix_clone = prefix.clone();
        self.for_each_key(
            |key| {
                let mut big_key = prefix.clone();
                big_key.extend(key);
                keys.push(big_key);
                Ok(())
            },
            prefix_clone,
        )
        .await?;
        Ok(keys)
    }

    /// Returns the number of keys of the map
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], String::from("Hello"));
    ///   map.insert(vec![1,2], String::from("Bonjour"));
    ///   map.insert(vec![2,2], String::from("Hallo"));
    ///   assert_eq!(map.count().await.unwrap(), 3);
    /// # })
    /// ```
    pub async fn count(&self) -> Result<usize, ViewError> {
        let mut count = 0;
        let prefix = Vec::new();
        self.for_each_key(
            |_key| {
                count += 1;
                Ok(())
            },
            prefix,
        )
        .await?;
        Ok(count)
    }

    /// The big map case
    async fn for_each_key_value_while_big<F>(
        context: &C,
        updates: &BTreeMap<Vec<u8>,Update<V>>,
        deletion_set: &DeletionSet,
        mut f: F,
        prefix: Vec<u8>,
    ) -> Result<(), ViewError>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool, ViewError> + Send,
    {
        let prefix_len = prefix.len();
        let mut updates = updates.range(get_interval(prefix.clone()));
        let mut update = updates.next();
        if !deletion_set.contains_prefix_of(&prefix) {
            let iter = deletion_set
                .deleted_prefixes
                .range(get_interval(prefix.clone()));
            let mut suffix_closed_set = SuffixClosedSetIterator::new(prefix_len, iter);
            let base = context.base_index(&prefix);
            for entry in context
                .find_key_values_by_prefix(&base)
                .await?
                .iterator()
            {
                let (index, bytes) = entry?;
                loop {
                    match update {
                        Some((key, value)) if &key[prefix_len..] <= index => {
                            if let Update::Set(value) = value {
                                let bytes = bcs::to_bytes(value)?;
                                if !f(&key[prefix_len..], &bytes)? {
                                    return Ok(());
                                }
                            }
                            update = updates.next();
                            if &key[prefix_len..] == index {
                                break;
                            }
                        }
                        _ => {
                            if !suffix_closed_set.find_key(index) && !f(index, bytes)? {
                                return Ok(());
                            }
                            break;
                        }
                    }
                }
            }
        }
        while let Some((key, value)) = update {
            if let Update::Set(value) = value {
                let bytes = bcs::to_bytes(value)?;
                if !f(&key[prefix_len..], &bytes)? {
                    return Ok(());
                }
            }
            update = updates.next();
        }
        Ok(())
    }

    /// Applies a function f on each index/value pair matching a prefix. Keys
    /// and values are visited in the lexicographic order. The shortened index
    /// is send to the function f and if it returns false then the loop ends
    /// prematurely
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], String::from("Hello"));
    ///   map.insert(vec![1,2], String::from("Bonjour"));
    ///   map.insert(vec![1,3], String::from("Hallo"));
    ///   let mut part_keys = Vec::new();
    ///   let prefix = vec![1];
    ///   map.for_each_key_value_while(|key, _value| {
    ///     part_keys.push(key.to_vec());
    ///     Ok(part_keys.len() < 2)
    ///   }, prefix).await.unwrap();
    ///   assert_eq!(part_keys.len(), 2);
    /// # })
    /// ```
    pub async fn for_each_key_value_while<F>(&self, mut f: F, prefix: Vec<u8>) -> Result<(), ViewError>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool, ViewError> + Send,
    {
        match &self.map_state {
            MapState::BigMap { updates, deletion_set } => {
                Self::for_each_key_value_while_big(&self.context, &updates, &deletion_set, f, prefix).await
            },
            MapState::SmallMap { small_map, .. } => {
		for (key, value) in small_map {
                    let value = bcs::to_bytes(value)?;
                    if !f(&key, &value)? {
                        return Ok(());
                    }
                }
                Ok(())
            },
        }
    }

    /// Applies a function f on each key/value pair matching a prefix. The shortened
    /// key and value are send to the function f. Keys and values are visited in the
    /// lexicographic order.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], String::from("Hello"));
    ///   let mut count = 0;
    ///   let prefix = Vec::new();
    ///   map.for_each_key_value(|_key, _value| {
    ///     count += 1;
    ///     Ok(())
    ///   }, prefix).await.unwrap();
    ///   assert_eq!(count, 1);
    /// # })
    /// ```
    pub async fn for_each_key_value<F>(&self, mut f: F, prefix: Vec<u8>) -> Result<(), ViewError>
    where
        F: FnMut(&[u8], &[u8]) -> Result<(), ViewError> + Send,
    {
        self.for_each_key_value_while(
            |key, value| {
                f(key, value)?;
                Ok(true)
            },
            prefix,
        )
        .await
    }
}

impl<C, V> ByteMapView<C, V>
where
    C: Context,
    ViewError: From<C::Error>,
    V: Sync + Send + Serialize + DeserializeOwned + 'static,
{
    /// Returns the list of keys and values of the map matching a prefix
    /// in lexicographic order.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![1,2], String::from("Hello"));
    ///   let prefix = vec![1];
    ///   assert_eq!(map.key_values_by_prefix(prefix).await.unwrap(), vec![(vec![1,2], String::from("Hello"))]);
    /// # })
    /// ```
    pub async fn key_values_by_prefix(
        &self,
        prefix: Vec<u8>,
    ) -> Result<Vec<(Vec<u8>, V)>, ViewError> {
        let mut key_values = Vec::new();
        let prefix_copy = prefix.clone();
        self.for_each_key_value(
            |key, value| {
                let value = bcs::from_bytes(value)?;
                let mut big_key = prefix.clone();
                big_key.extend(key);
                key_values.push((big_key, value));
                Ok(())
            },
            prefix_copy,
        )
        .await?;
        Ok(key_values)
    }

    /// Returns the list of keys and values of the map in lexicographic order.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![1,2], String::from("Hello"));
    ///   assert_eq!(map.key_values().await.unwrap(), vec![(vec![1,2], String::from("Hello"))]);
    /// # })
    /// ```
    pub async fn key_values(&self) -> Result<Vec<(Vec<u8>, V)>, ViewError> {
        self.key_values_by_prefix(Vec::new()).await
    }
}

impl<C, V> ByteMapView<C, V>
where
    C: Context + Sync,
    ViewError: From<C::Error>,
    V: Default + DeserializeOwned + 'static,
{
    /// Obtains a mutable reference to a value at a given position.
    /// Default value if the index is missing.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::ByteMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = ByteMapView::load(context).await.unwrap();
    ///   map.insert(vec![0,1], String::from("Hello"));
    ///   assert_eq!(map.get_mut_or_default(&[7]).await.unwrap(), "");
    ///   let value = map.get_mut_or_default(&[0,1]).await.unwrap();
    ///   assert_eq!(*value, String::from("Hello"));
    ///   *value = String::from("Hola");
    ///   assert_eq!(map.get(&[0,1]).await.unwrap(), Some(String::from("Hola")));
    /// # })
    /// ```
    pub async fn get_mut_or_default(&mut self, short_key: &[u8]) -> Result<&mut V, ViewError> {
        match &mut self.map_state {
            MapState::BigMap { updates, deletion_set } => {
                let update = match updates.entry(short_key.to_vec()) {
                    Entry::Vacant(e) if deletion_set.contains_prefix_of(short_key) => {
                        e.insert(Update::Set(V::default()))
                    }
                    Entry::Vacant(e) => {
                        let key = self.context.base_tag_index(KeyTag::Index as u8, short_key);
                        let value = self.context.read_value(&key).await?.unwrap_or_default();
                        e.insert(Update::Set(value))
                    }
                    Entry::Occupied(entry) => {
                        let entry = entry.into_mut();
                        match entry {
                            Update::Set(_) => &mut *entry,
                            Update::Removed => {
                                *entry = Update::Set(V::default());
                                &mut *entry
                            }
                        }
                    }
                };
                let Update::Set(value) = update else {
                    unreachable!()
                };
                Ok(value)
            },
            MapState::SmallMap { small_map, .. } => {
                Ok(match small_map.entry(short_key.to_vec()) {
                    Entry::Vacant(entry) => {
                        entry.insert(V::default())
                    },
                    Entry::Occupied(entry) => {
                        let entry = entry.into_mut();
                        &mut *entry
                    },
                })
            },
        }
    }
}

#[async_trait]
impl<C, V> HashableView<C> for ByteMapView<C, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    V: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.hash().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        #[cfg(with_metrics)]
        let _hash_latency = MAP_VIEW_HASH_RUNTIME.measure_latency();
        let mut hasher = sha3::Sha3_256::default();
        let mut count = 0;
        let prefix = Vec::new();
        self.for_each_key_value(
            |index, value| {
                count += 1;
                hasher.update_with_bytes(index)?;
                hasher.update_with_bytes(value)?;
                Ok(())
            },
            prefix,
        )
        .await?;
        hasher.update_with_bcs_bytes(&count)?;
        Ok(hasher.finalize())
    }
}

/// A `View` that has a type for keys. The ordering of the entries
/// is determined by the serialization of the context.
#[derive(Debug)]
pub struct MapView<C, I, V> {
    map: ByteMapView<C, V>,
    _phantom: PhantomData<I>,
}

#[async_trait]
impl<C, I, V> View<C> for MapView<C, I, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + Serialize,
    V: Send + Sync + Serialize + DeserializeOwned + Clone,
{
    const NUM_INIT_KEYS: usize = ByteMapView::<C, V>::NUM_INIT_KEYS;

    fn context(&self) -> &C {
        self.map.context()
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        ByteMapView::<C, V>::pre_load(context)
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let map = ByteMapView::post_load(context, values)?;
        Ok(MapView {
            map,
            _phantom: PhantomData,
        })
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        Self::post_load(context, &[])
    }

    fn rollback(&mut self) {
        self.map.rollback()
    }

    async fn has_pending_changes(&self) -> bool {
        self.map.has_pending_changes().await
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        self.map.flush(batch)
    }

    fn clear(&mut self) {
        self.map.clear()
    }
}

impl<C, I, V> ClonableView<C> for MapView<C, I, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + Serialize,
    V: Clone + Send + Sync + Serialize + DeserializeOwned,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(MapView {
            map: self.map.clone_unchecked()?,
            _phantom: PhantomData,
        })
    }
}

impl<C, I, V> MapView<C, I, V>
where
    C: Context + Sync,
    ViewError: From<C::Error>,
    V: Clone,
    I: Serialize,
{
    /// Inserts or resets a value at an index.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : MapView<_,u32,_> = MapView::load(context).await.unwrap();
    ///   map.insert(&(24 as u32), String::from("Hello"));
    ///   assert_eq!(map.get(&(24 as u32)).await.unwrap(), Some(String::from("Hello")));
    /// # })
    /// ```
    pub fn insert<Q>(&mut self, index: &Q, value: V) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.map.insert(short_key, value);
        Ok(())
    }

    /// Removes a value. If absent then the operation does nothing.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = MapView::<_,u32,String>::load(context).await.unwrap();
    ///   map.remove(&(37 as u32));
    ///   assert_eq!(map.get(&(37 as u32)).await.unwrap(), None);
    /// # })
    /// ```
    pub fn remove<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.map.remove(short_key);
        Ok(())
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.map.extra()
    }

    /// Returns `true` if the map contains a value for the specified key.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = MapView::<_,u32,String>::load(context).await.unwrap();
    ///   map.insert(&(37 as u32), String::from("Hello"));
    ///   assert!(map.contains_key(&(37 as u32)).await.unwrap());
    ///   assert!(!map.contains_key(&(34 as u32)).await.unwrap());
    /// # })
    /// ```
    pub async fn contains_key<Q>(&self, index: &Q) -> Result<bool, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.map.contains_key(&short_key).await
    }
}

impl<C, I, V> MapView<C, I, V>
where
    C: Context + Sync,
    ViewError: From<C::Error>,
    I: Serialize,
    V: Clone + DeserializeOwned + 'static,
{
    /// Reads the value at the given position, if any.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : MapView<_, u32,_> = MapView::load(context).await.unwrap();
    ///   map.insert(&(37 as u32), String::from("Hello"));
    ///   assert_eq!(map.get(&(37 as u32)).await.unwrap(), Some(String::from("Hello")));
    ///   assert_eq!(map.get(&(34 as u32)).await.unwrap(), None);
    /// # })
    /// ```
    pub async fn get<Q>(&self, index: &Q) -> Result<Option<V>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.map.get(&short_key).await
    }

    /// Obtains a mutable reference to a value at a given position if available
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : MapView::<_,u32,String> = MapView::load(context).await.unwrap();
    ///   map.insert(&(37 as u32), String::from("Hello"));
    ///   assert_eq!(map.get_mut(&(34 as u32)).await.unwrap(), None);
    ///   let value = map.get_mut(&(37 as u32)).await.unwrap().unwrap();
    ///   *value = String::from("Hola");
    ///   assert_eq!(map.get(&(37 as u32)).await.unwrap(), Some(String::from("Hola")));
    /// # })
    /// ```
    pub async fn get_mut<Q>(&mut self, index: &Q) -> Result<Option<&mut V>, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.map.get_mut(&short_key).await
    }
}

impl<C, I, V> MapView<C, I, V>
where
    C: Context + Sync,
    ViewError: From<C::Error>,
    I: Sync + Send + Serialize + DeserializeOwned,
    V: Sync + Serialize + DeserializeOwned + 'static,
{
    /// Returns the list of indices in the map. The order is determined by serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : MapView::<_,u32,String> = MapView::load(context).await.unwrap();
    ///   map.insert(&(37 as u32), String::from("Hello"));
    ///   assert_eq!(map.indices().await.unwrap(), vec![37 as u32]);
    /// # })
    /// ```
    pub async fn indices(&self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::<I>::new();
        self.for_each_index(|index: I| {
            indices.push(index);
            Ok(())
        })
        .await?;
        Ok(indices)
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the serialization. If the function returns false, then
    /// the loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : MapView<_, u128, String> = MapView::load(context).await.unwrap();
    ///   map.insert(&(34 as u128), String::from("Thanks"));
    ///   map.insert(&(37 as u128), String::from("Spasiba"));
    ///   map.insert(&(38 as u128), String::from("Merci"));
    ///   let mut count = 0;
    ///   map.for_each_index_while(|_index| {
    ///     count += 1;
    ///     Ok(count < 2)
    ///   }).await.unwrap();
    ///   assert_eq!(count, 2);
    /// # })
    /// ```
    pub async fn for_each_index_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<bool, ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map
            .for_each_key_while(
                |key| {
                    let index = C::deserialize_value(key)?;
                    f(index)
                },
                prefix,
            )
            .await?;
        Ok(())
    }

    /// Applies a function f on each index. Indices are visited in the order
    /// determined by serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : MapView<_, u128, String> = MapView::load(context).await.unwrap();
    ///   map.insert(&(34 as u128), String::from("Hello"));
    ///   let mut count = 0;
    ///   map.for_each_index(|_index| {
    ///     count += 1;
    ///     Ok(())
    ///   }).await.unwrap();
    ///   assert_eq!(count, 1);
    /// # })
    /// ```
    pub async fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map
            .for_each_key(
                |key| {
                    let index = C::deserialize_value(key)?;
                    f(index)
                },
                prefix,
            )
            .await?;
        Ok(())
    }

    /// Applies a function f on each index/value pair. Indices and values are
    /// visited in an order determined by serialization.
    /// If the function returns false, then the loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : MapView<_, u128, String> = MapView::load(context).await.unwrap();
    ///   map.insert(&(34 as u128), String::from("Thanks"));
    ///   map.insert(&(37 as u128), String::from("Spasiba"));
    ///   map.insert(&(38 as u128), String::from("Merci"));
    ///   let mut values = Vec::new();
    ///   map.for_each_index_value_while(|_index, value| {
    ///     values.push(value);
    ///     Ok(values.len() < 2)
    ///   }).await.unwrap();
    ///   assert_eq!(values.len(), 2);
    /// # })
    /// ```
    pub async fn for_each_index_value_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I, V) -> Result<bool, ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map
            .for_each_key_value_while(
                |key, bytes| {
                    let index = C::deserialize_value(key)?;
                    let value = C::deserialize_value(bytes)?;
                    f(index, value)
                },
                prefix,
            )
            .await?;
        Ok(())
    }

    /// Applies a function on each index/value pair. Indices and values are
    /// visited in an order determined by serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : MapView<_,Vec<u8>,_> = MapView::load(context).await.unwrap();
    ///   map.insert(&vec![0,1], String::from("Hello"));
    ///   let mut count = 0;
    ///   map.for_each_index_value(|_index, _value| {
    ///     count += 1;
    ///     Ok(())
    ///   }).await.unwrap();
    ///   assert_eq!(count, 1);
    /// # })
    /// ```
    pub async fn for_each_index_value<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I, V) -> Result<(), ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map
            .for_each_key_value(
                |key, bytes| {
                    let index = C::deserialize_value(key)?;
                    let value = C::deserialize_value(bytes)?;
                    f(index, value)
                },
                prefix,
            )
            .await?;
        Ok(())
    }
}

impl<C, I, V> MapView<C, I, V>
where
    C: Context + Sync,
    ViewError: From<C::Error>,
    I: Serialize,
    V: Default + DeserializeOwned + 'static,
{
    /// Obtains a mutable reference to a value at a given position.
    /// Default value if the index is missing.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : MapView<_,u32,u128> = MapView::load(context).await.unwrap();
    ///   let value = map.get_mut_or_default(&(34 as u32)).await.unwrap();
    ///   assert_eq!(*value, 0 as u128);
    /// # })
    /// ```
    pub async fn get_mut_or_default<Q>(&mut self, index: &Q) -> Result<&mut V, ViewError>
    where
        I: Borrow<Q>,
        Q: Sync + Send + Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.map.get_mut_or_default(&short_key).await
    }
}

#[async_trait]
impl<C, I, V> HashableView<C> for MapView<C, I, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + Serialize + DeserializeOwned,
    V: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.map.hash_mut().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.map.hash().await
    }
}

/// A Custom MapView that uses the custom serialization
#[derive(Debug)]
pub struct CustomMapView<C, I, V> {
    map: ByteMapView<C, V>,
    _phantom: PhantomData<I>,
}

#[async_trait]
impl<C, I, V> View<C> for CustomMapView<C, I, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + CustomSerialize,
    V: Clone + Send + Sync + Serialize + DeserializeOwned,
{
    const NUM_INIT_KEYS: usize = ByteMapView::<C, V>::NUM_INIT_KEYS;

    fn context(&self) -> &C {
        self.map.context()
    }

    fn pre_load(context: &C) -> Result<Vec<Vec<u8>>, ViewError> {
        ByteMapView::<C, V>::pre_load(context)
    }

    fn post_load(context: C, values: &[Option<Vec<u8>>]) -> Result<Self, ViewError> {
        let map = ByteMapView::post_load(context, values)?;
        Ok(CustomMapView {
            map,
            _phantom: PhantomData,
        })
    }

    async fn load(context: C) -> Result<Self, ViewError> {
        Self::post_load(context, &[])
    }

    fn rollback(&mut self) {
        self.map.rollback()
    }

    async fn has_pending_changes(&self) -> bool {
        self.map.has_pending_changes().await
    }

    fn flush(&mut self, batch: &mut Batch) -> Result<bool, ViewError> {
        self.map.flush(batch)
    }

    fn clear(&mut self) {
        self.map.clear()
    }
}

impl<C, I, V> ClonableView<C> for CustomMapView<C, I, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + CustomSerialize,
    V: Clone + Send + Sync + Serialize + DeserializeOwned,
{
    fn clone_unchecked(&mut self) -> Result<Self, ViewError> {
        Ok(CustomMapView {
            map: self.map.clone_unchecked()?,
            _phantom: PhantomData,
        })
    }
}

impl<C, I, V> CustomMapView<C, I, V>
where
    C: Context + Sync,
    ViewError: From<C::Error>,
    V: Clone,
    I: CustomSerialize,
{
    /// Insert or resets a value.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : MapView<_,u128,_> = MapView::load(context).await.unwrap();
    ///   map.insert(&(24 as u128), String::from("Hello"));
    ///   assert_eq!(map.get(&(24 as u128)).await.unwrap(), Some(String::from("Hello")));
    /// # })
    /// ```
    pub fn insert<Q>(&mut self, index: &Q, value: V) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.map.insert(short_key, value);
        Ok(())
    }

    /// Removes a value. If absent then this does not do anything.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = MapView::<_,u128,String>::load(context).await.unwrap();
    ///   map.remove(&(37 as u128));
    ///   assert_eq!(map.get(&(37 as u128)).await.unwrap(), None);
    /// # })
    /// ```
    pub fn remove<Q>(&mut self, index: &Q) -> Result<(), ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.map.remove(short_key);
        Ok(())
    }

    /// Obtains the extra data.
    pub fn extra(&self) -> &C::Extra {
        self.map.extra()
    }

    /// Returns `true` if the map contains a value for the specified key.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = MapView::<_,u128,String>::load(context).await.unwrap();
    ///   map.insert(&(37 as u128), String::from("Hello"));
    ///   assert!(map.contains_key(&(37 as u128)).await.unwrap());
    ///   assert!(!map.contains_key(&(34 as u128)).await.unwrap());
    /// # })
    /// ```
    pub async fn contains_key<Q>(&self, index: &Q) -> Result<bool, ViewError>
    where
        I: Borrow<Q>,
        Q: Serialize + ?Sized,
    {
        let short_key = C::derive_short_key(index)?;
        self.map.contains_key(&short_key).await
    }
}

impl<C, I, V> CustomMapView<C, I, V>
where
    C: Context + Sync,
    ViewError: From<C::Error>,
    I: CustomSerialize,
    V: Clone + DeserializeOwned + 'static,
{
    /// Reads the value at the given position, if any.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::CustomMapView;
    /// # use linera_views::memory::MemoryContext;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : CustomMapView<MemoryContext<()>, u128, String> = CustomMapView::load(context).await.unwrap();
    ///   map.insert(&(34 as u128), String::from("Hello"));
    ///   assert_eq!(map.get(&(34 as u128)).await.unwrap(), Some(String::from("Hello")));
    /// # })
    /// ```
    pub async fn get<Q>(&self, index: &Q) -> Result<Option<V>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.map.get(&short_key).await
    }

    /// Obtains a mutable reference to a value at a given position if available
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::CustomMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : CustomMapView<_, u128, String> = CustomMapView::load(context).await.unwrap();
    ///   map.insert(&(34 as u128), String::from("Hello"));
    ///   let value = map.get_mut(&(34 as u128)).await.unwrap().unwrap();
    ///   *value = String::from("Hola");
    ///   assert_eq!(map.get(&(34 as u128)).await.unwrap(), Some(String::from("Hola")));
    /// # })
    /// ```
    pub async fn get_mut<Q>(&mut self, index: &Q) -> Result<Option<&mut V>, ViewError>
    where
        I: Borrow<Q>,
        Q: CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.map.get_mut(&short_key).await
    }
}

impl<C, I, V> CustomMapView<C, I, V>
where
    C: Context + Sync,
    ViewError: From<C::Error>,
    I: Sync + Send + CustomSerialize,
    V: Sync + Serialize + DeserializeOwned + 'static,
{
    /// Returns the list of indices in the map. The order is determined
    /// by the custom serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::MapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : MapView::<_,u128,String> = MapView::load(context).await.unwrap();
    ///   map.insert(&(34 as u128), String::from("Hello"));
    ///   map.insert(&(37 as u128), String::from("Bonjour"));
    ///   assert_eq!(map.indices().await.unwrap(), vec![34 as u128,37 as u128]);
    /// # })
    /// ```
    pub async fn indices(&self) -> Result<Vec<I>, ViewError> {
        let mut indices = Vec::<I>::new();
        self.for_each_index(|index: I| {
            indices.push(index);
            Ok(())
        })
        .await?;
        Ok(indices)
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the custom serialization. If the function returns false,
    /// then the loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::CustomMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = CustomMapView::load(context).await.unwrap();
    ///   map.insert(&(34 as u128), String::from("Hello"));
    ///   map.insert(&(37 as u128), String::from("Hola"));
    ///   let mut indices = Vec::<u128>::new();
    ///   map.for_each_index_while(|index| {
    ///     indices.push(index);
    ///     Ok(indices.len() < 5)
    ///   }).await.unwrap();
    ///   assert_eq!(indices.len(), 2);
    /// # })
    /// ```
    pub async fn for_each_index_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<bool, ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map
            .for_each_key_while(
                |key| {
                    let index = I::from_custom_bytes(key)?;
                    f(index)
                },
                prefix,
            )
            .await?;
        Ok(())
    }

    /// Applies a function f on each index. Indices are visited in an order
    /// determined by the custom serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::CustomMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = CustomMapView::load(context).await.unwrap();
    ///   map.insert(&(34 as u128), String::from("Hello"));
    ///   map.insert(&(37 as u128), String::from("Hola"));
    ///   let mut indices = Vec::<u128>::new();
    ///   map.for_each_index(|index| {
    ///     indices.push(index);
    ///     Ok(())
    ///   }).await.unwrap();
    ///   assert_eq!(indices, vec![34,37]);
    /// # })
    /// ```
    pub async fn for_each_index<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I) -> Result<(), ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map
            .for_each_key(
                |key| {
                    let index = I::from_custom_bytes(key)?;
                    f(index)
                },
                prefix,
            )
            .await?;
        Ok(())
    }

    /// Applies a function f on the index/value pairs. Indices and values are
    /// visited in an order determined by the custom serialization.
    /// If the function returns false, then the loop ends prematurely.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::CustomMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map = CustomMapView::<_,u128,String>::load(context).await.unwrap();
    ///   map.insert(&(34 as u128), String::from("Hello"));
    ///   map.insert(&(37 as u128), String::from("Hola"));
    ///   let mut values = Vec::new();
    ///   map.for_each_index_value_while(|_index, value| {
    ///     values.push(value);
    ///     Ok(values.len() < 5)
    ///   }).await.unwrap();
    ///   assert_eq!(values.len(), 2);
    /// # })
    /// ```
    pub async fn for_each_index_value_while<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I, V) -> Result<bool, ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map
            .for_each_key_value_while(
                |key, bytes| {
                    let index = I::from_custom_bytes(key)?;
                    let value = C::deserialize_value(bytes)?;
                    f(index, value)
                },
                prefix,
            )
            .await?;
        Ok(())
    }

    /// Applies a function f on each index/value pair. Indices and values are
    /// visited in an order determined by the custom serialization.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::CustomMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : CustomMapView<_, u128, String> = CustomMapView::load(context).await.unwrap();
    ///   map.insert(&(34 as u128), String::from("Hello"));
    ///   map.insert(&(37 as u128), String::from("Hola"));
    ///   let mut indices = Vec::<u128>::new();
    ///   map.for_each_index_value(|index, _value| {
    ///     indices.push(index);
    ///     Ok(())
    ///   }).await.unwrap();
    ///   assert_eq!(indices, vec![34,37]);
    /// # })
    /// ```
    pub async fn for_each_index_value<F>(&self, mut f: F) -> Result<(), ViewError>
    where
        F: FnMut(I, V) -> Result<(), ViewError> + Send,
    {
        let prefix = Vec::new();
        self.map
            .for_each_key_value(
                |key, bytes| {
                    let index = I::from_custom_bytes(key)?;
                    let value = C::deserialize_value(bytes)?;
                    f(index, value)
                },
                prefix,
            )
            .await?;
        Ok(())
    }
}

impl<C, I, V> CustomMapView<C, I, V>
where
    C: Context + Sync,
    ViewError: From<C::Error>,
    I: CustomSerialize,
    V: Default + DeserializeOwned + 'static,
{
    /// Obtains a mutable reference to a value at a given position.
    /// Default value if the index is missing.
    /// ```rust
    /// # tokio_test::block_on(async {
    /// # use linera_views::memory::create_memory_context;
    /// # use linera_views::map_view::CustomMapView;
    /// # use crate::linera_views::views::View;
    /// # let context = create_memory_context();
    ///   let mut map : CustomMapView<_, u128, String> = CustomMapView::load(context).await.unwrap();
    ///   assert_eq!(*map.get_mut_or_default(&(34 as u128)).await.unwrap(), String::new());
    /// # })
    /// ```
    pub async fn get_mut_or_default<Q>(&mut self, index: &Q) -> Result<&mut V, ViewError>
    where
        I: Borrow<Q>,
        Q: Sync + Send + Serialize + CustomSerialize,
    {
        let short_key = index.to_custom_bytes()?;
        self.map.get_mut_or_default(&short_key).await
    }
}

#[async_trait]
impl<C, I, V> HashableView<C> for CustomMapView<C, I, V>
where
    C: Context + Send + Sync,
    ViewError: From<C::Error>,
    I: Send + Sync + CustomSerialize,
    V: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    type Hasher = sha3::Sha3_256;

    async fn hash_mut(&mut self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.map.hash_mut().await
    }

    async fn hash(&self) -> Result<<Self::Hasher as Hasher>::Output, ViewError> {
        self.map.hash().await
    }
}

/// Type wrapping `ByteMapView` while memoizing the hash.
pub type HashedByteMapView<C, V> = WrappedHashableContainerView<C, ByteMapView<C, V>, HasherOutput>;

/// Type wrapping `MapView` while memoizing the hash.
pub type HashedMapView<C, I, V> = WrappedHashableContainerView<C, MapView<C, I, V>, HasherOutput>;

/// Type wrapping `CustomMapView` while memoizing the hash.
pub type HashedCustomMapView<C, I, V> =
    WrappedHashableContainerView<C, CustomMapView<C, I, V>, HasherOutput>;

#[cfg(test)]
pub mod tests {
    use std::borrow::Borrow;

    fn check_str<T: Borrow<str>>(s: T) {
        let ser1 = bcs::to_bytes("Hello").unwrap();
        let ser2 = bcs::to_bytes(s.borrow()).unwrap();
        assert_eq!(ser1, ser2);
    }

    fn check_array_u8<T: Borrow<[u8]>>(v: T) {
        let ser1 = bcs::to_bytes(&vec![23_u8, 67_u8, 123_u8]).unwrap();
        let ser2 = bcs::to_bytes(&v.borrow()).unwrap();
        assert_eq!(ser1, ser2);
    }

    #[test]
    fn test_serialization_borrow() {
        check_str("Hello".to_string());
        check_str("Hello");
        //
        check_array_u8(vec![23, 67, 123]);
        check_array_u8([23, 67, 123]);
    }
}
