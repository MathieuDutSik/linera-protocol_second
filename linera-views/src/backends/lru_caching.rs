// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Add LRU (least recently used) caching to a given store.

#[cfg(with_metrics)]
use std::sync::LazyLock;
use std::{
    collections::{btree_map, hash_map::RandomState, BTreeMap},
    sync::{Arc, Mutex},
};

use linked_hash_map::LinkedHashMap;
use serde::{Deserialize, Serialize};
#[cfg(with_metrics)]
use {linera_base::prometheus_util::register_int_counter_vec, prometheus::IntCounterVec};

use crate::{
    batch::{Batch, WriteOperation},
    common::get_interval,
    store::{AdminKeyValueStore, ReadableKeyValueStore, WithError, WritableKeyValueStore},
};
#[cfg(with_testing)]
use crate::{memory::MemoryStore, store::TestKeyValueStore};

#[cfg(with_metrics)]
/// The total number of cache read value misses
static READ_VALUE_CACHE_MISS_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec(
        "num_read_value_cache_miss",
        "Number of read value cache misses",
        &[],
    )
});

#[cfg(with_metrics)]
/// The total number of read value cache hits
static READ_VALUE_CACHE_HIT_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec(
        "num_read_value_cache_hits",
        "Number of read value cache hits",
        &[],
    )
});

#[cfg(with_metrics)]
/// The total number of contains key cache misses
static CONTAINS_KEY_CACHE_MISS_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec(
        "num_contains_key_cache_miss",
        "Number of contains key cache misses",
        &[],
    )
});

#[cfg(with_metrics)]
/// The total number of contains key cache hits
static CONTAINS_KEY_CACHE_HIT_COUNT: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec(
        "num_contains_key_cache_hit",
        "Number of contains key cache hits",
        &[],
    )
});

/// The parametrization of the cache
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageCacheConfig {
    /// The maximum size of the cache, in bytes (keys size + value sizes)
    pub max_cache_size: usize,
    /// The maximum size of an entry size, in bytes
    pub max_entry_size: usize,
    /// The maximum number of entries in the cache.
    pub max_cache_entries: usize,
}

/// The maximum number of entries in the cache.
/// If the number of entries in the cache is too large then the underlying maps
/// become the limiting factor
pub const DEFAULT_STORAGE_CACHE_CONFIG: StorageCacheConfig = StorageCacheConfig {
    max_cache_size: 10000000,
    max_entry_size: 1000000,
    max_cache_entries: 1000,
};

#[derive(Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
enum CacheEntry {
    Find,
    Value,
}

enum ValueCacheEntry {
    DoesNotExist,
    Exists,
    Value(Vec<u8>),
}

impl ValueCacheEntry {
    fn size(&self) -> usize {
        match self {
            ValueCacheEntry::Value(vec) => vec.len(),
            _ => 0,
        }
    }
}

struct FindCacheEntry {
    map: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl FindCacheEntry {
    fn get_find_keys(&self, key_prefix: &[u8]) -> Vec<Vec<u8>> {
        let key_prefix = key_prefix.to_vec();
        let delta = key_prefix.len();
        self.map
            .range(get_interval(key_prefix))
            .map(|(key, _)| key[delta..].to_vec())
            .collect::<Vec<_>>()
    }

    fn get_find_key_values(&self, key_prefix: &[u8]) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
        let key_prefix = key_prefix.to_vec();
        let delta = key_prefix.len();
        Some(
            self.map
                .range(get_interval(key_prefix))
                .map(|(key, value)| (key[delta..].to_vec(), value.to_vec()))
                .collect::<Vec<_>>(),
        )
    }

    fn get_contains_key(&self, key: &[u8]) -> bool {
        self.map.contains_key(key)
    }

    fn get_read_value(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        Some(self.map.get(key).cloned())
    }

    fn update_cache_entry(&mut self, key: &[u8], new_value: Option<Vec<u8>>) {
        match new_value {
            None => self.map.remove(key),
            Some(new_value) => self.map.insert(key.to_vec(), new_value),
        };
    }

    fn delete_prefix(&mut self, key_prefix: &[u8]) {
        let keys = self.map
            .range(get_interval(key_prefix.to_vec()))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in keys {
            self.map.remove(&key);
        }
    }

    fn size(&self) -> usize {
        let mut total_size = 0;
        for (key, value) in &self.map {
            total_size += key.len() + value.len();
        }
        total_size
    }
}

/// Stores the data for simple `read_values` queries.
///
/// This data structure is inspired by the crate `lru-cache` but was modified to support
/// range deletions.
struct LruPrefixCache {
    map_find: BTreeMap<Vec<u8>, FindCacheEntry>,
    map_value: BTreeMap<Vec<u8>, ValueCacheEntry>,
    queue: LinkedHashMap<(Vec<u8>, CacheEntry), usize, RandomState>,
    storage_cache_config: StorageCacheConfig,
    total_size: usize,
    /// Whether we have exclusive R/W access to the keys under the root key of the store.
    has_exclusive_access: bool,
}

impl LruPrefixCache {
    /// Creates an `LruPrefixCache`.
    pub fn new(storage_cache_config: StorageCacheConfig) -> Self {
        Self {
            map_find: BTreeMap::new(),
            map_value: BTreeMap::new(),
            queue: LinkedHashMap::new(),
            storage_cache_config,
            total_size: 0,
            has_exclusive_access: false,
        }
    }

    /// Find the lower bound for the key if existing.
    fn get_lower_bound_mut(&mut self, key: &[u8]) -> Option<(&Vec<u8>, &mut FindCacheEntry)> {
        match self.map_find.range_mut(..=key.to_vec()).next_back() {
            None => None,
            Some((key_store, value)) => {
                if key.starts_with(key_store) {
                    Some((key_store, value))
                } else {
                    None
                }
            }
        }
    }

    /// Trim the cache so that it fits within the contraints.
    fn trim_cache(&mut self) {
        while self.total_size > self.storage_cache_config.max_cache_size
            || self.queue.len() > self.storage_cache_config.max_cache_entries
        {
            let Some(((key, nature), key_value_size)) = self.queue.pop_front() else {
                break;
            };
            match nature {
                CacheEntry::Find => {
                    self.map_find.remove(&key);
                }
                CacheEntry::Value => {
                    self.map_value.remove(&key);
                }
            }
            self.total_size -= key_value_size;
        }
    }

    /// Inserts an entry into the cache.
    pub fn insert_value(&mut self, key: Vec<u8>, cache_entry: ValueCacheEntry) {
        let key_value_size = key.len() + cache_entry.size();
        let full_key = (key.clone(), CacheEntry::Value);
        if (matches!(cache_entry, ValueCacheEntry::DoesNotExist) && !self.has_exclusive_access)
            || key_value_size > self.storage_cache_config.max_entry_size
        {
            // Just forget about the entry.
            if let Some(old_key_value_size) = self.queue.remove(&full_key) {
                self.total_size -= old_key_value_size;
                self.map_value.remove(&key);
            };
            return;
        }
        match self.map_value.entry(key) {
            btree_map::Entry::Occupied(mut entry) => {
                entry.insert(cache_entry);
                // Put it on first position for LRU
                let Some(old_key_value_size) = self.queue.remove(&full_key) else {
                    unreachable!("The entry should be present in the queue");
                };
                self.total_size -= old_key_value_size;
                self.queue.insert(full_key, key_value_size);
                self.total_size += key_value_size;
            }
            btree_map::Entry::Vacant(entry) => {
                entry.insert(cache_entry);
                self.queue.insert(full_key, key_value_size);
                self.total_size += key_value_size;
            }
        }
        self.trim_cache();
    }

    /// Correct the find entries
    pub fn correct_find_entry(&mut self, key: &[u8], new_value: Option<Vec<u8>>) {
        let lower_bound = self.get_lower_bound_mut(key);
        if let Some((lower_bound, cache_entry)) = lower_bound {
            let key_red = &key[lower_bound.len()..];
            cache_entry.update_cache_entry(key_red, new_value);
        }
    }

    /// Inserts a read_value entry into the cache.
    pub fn insert_read_value(&mut self, key: Vec<u8>, value: &Option<Vec<u8>>) {
        let cache_entry = match value {
            None => ValueCacheEntry::DoesNotExist,
            Some(vec) => ValueCacheEntry::Value(vec.to_vec()),
        };
        self.insert_value(key, cache_entry)
    }

    /// Inserts a read_value entry into the cache.
    pub fn insert_contains_key(&mut self, key: Vec<u8>, result: bool) {
        let cache_entry = match result {
            false => ValueCacheEntry::DoesNotExist,
            true => ValueCacheEntry::Exists,
        };
        self.insert_value(key, cache_entry)
    }

    /// Marks cached keys that match the prefix as deleted. Importantly, this does not
    /// create new entries in the cache.
    pub fn delete_prefix(&mut self, key_prefix: &[u8]) {
        if self.has_exclusive_access {
            for (key, value) in self.map_value.range_mut(get_interval(key_prefix.to_vec())) {
                let full_key = (key.clone(), CacheEntry::Value);
                *self.queue.get_mut(&full_key).unwrap() = key.len();
                self.total_size -= value.size();
                *value = ValueCacheEntry::DoesNotExist;
            }
        } else {
            // Just forget about the entries.
            let mut keys = Vec::new();
            for (key, _) in self.map_value.range(get_interval(key_prefix.to_vec())) {
                keys.push(key.to_vec());
            }
            for key in keys {
                self.map_value.remove(&key);
                let full_key = (key, CacheEntry::Value);
                let Some(key_value_size) = self.queue.remove(&full_key) else {
                    unreachable!("The key should be in the queue");
                };
                self.total_size -= key_value_size;
            }
        }
    }

    /// Returns the cached value, or `Some(None)` if the entry does not exist in the
    /// database. If `None` is returned, the entry might exist in the database but is
    /// not in the cache.
    pub fn query_read_value(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        let result = match self.map_value.get(key) {
            None => None,
            Some(entry) => entry.get_read_value(),
        };
        if let Some(result) = result {
            let full_key = (key.to_vec(), CacheEntry::Value);
            let entry = self.queue.remove(&full_key).unwrap();
            self.queue.insert(full_key, entry);
            return Some(result);
        }



        
        match self.map_value.get(key) {
            None => None,
            Some(entry) => match entry {
                ValueCacheEntry::DoesNotExist => Some(None),
                ValueCacheEntry::Exists => None,
                ValueCacheEntry::Value(vec) => Some(Some(vec.clone())),
            },
        }
    }

    /// Returns `Some(true)` or `Some(false)` if we know that the entry does or does not
    /// exist in the database. Returns `None` if that information is not in the cache.
    pub fn query_contains_key(&self, key: &[u8]) -> Option<bool> {
        self.map_value
            .get(key)
            .map(|entry| !matches!(entry, ValueCacheEntry::DoesNotExist))
    }
}

/// We take a store, a maximum size and build a LRU-based system.
#[derive(Clone)]
pub struct LruCachingStore<K> {
    /// The inner store that is called by the LRU cache one
    store: K,
    /// The LRU cache of values.
    cache: Option<Arc<Mutex<LruPrefixCache>>>,
}

impl<K> WithError for LruCachingStore<K>
where
    K: WithError,
{
    type Error = K::Error;
}

impl<K> ReadableKeyValueStore for LruCachingStore<K>
where
    K: ReadableKeyValueStore + Send + Sync,
{
    // The LRU cache does not change the underlying store's size limits.
    const MAX_KEY_SIZE: usize = K::MAX_KEY_SIZE;
    type Keys = K::Keys;
    type KeyValues = K::KeyValues;

    fn max_stream_queries(&self) -> usize {
        self.store.max_stream_queries()
    }

    async fn read_value_bytes(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let Some(cache) = &self.cache else {
            return self.store.read_value_bytes(key).await;
        };
        // First inquiring in the read_value_bytes LRU
        {
            let cache = cache.lock().unwrap();
            if let Some(value) = cache.query_read_value(key) {
                #[cfg(with_metrics)]
                READ_VALUE_CACHE_HIT_COUNT.with_label_values(&[]).inc();
                return Ok(value);
            }
        }
        #[cfg(with_metrics)]
        READ_VALUE_CACHE_MISS_COUNT.with_label_values(&[]).inc();
        let value = self.store.read_value_bytes(key).await?;
        let mut cache = cache.lock().unwrap();
        cache.insert_read_value(key.to_vec(), &value);
        Ok(value)
    }

    async fn contains_key(&self, key: &[u8]) -> Result<bool, Self::Error> {
        let Some(cache) = &self.cache else {
            return self.store.contains_key(key).await;
        };
        {
            let cache = cache.lock().unwrap();
            if let Some(value) = cache.query_contains_key(key) {
                #[cfg(with_metrics)]
                CONTAINS_KEY_CACHE_HIT_COUNT.with_label_values(&[]).inc();
                return Ok(value);
            }
        }
        #[cfg(with_metrics)]
        CONTAINS_KEY_CACHE_MISS_COUNT.with_label_values(&[]).inc();
        let result = self.store.contains_key(key).await?;
        let mut cache = cache.lock().unwrap();
        cache.insert_contains_key(key.to_vec(), result);
        Ok(result)
    }

    async fn contains_keys(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>, Self::Error> {
        let Some(cache) = &self.cache else {
            return self.store.contains_keys(keys).await;
        };
        let size = keys.len();
        let mut results = vec![false; size];
        let mut indices = Vec::new();
        let mut key_requests = Vec::new();
        {
            let cache = cache.lock().unwrap();
            for i in 0..size {
                if let Some(value) = cache.query_contains_key(&keys[i]) {
                    #[cfg(with_metrics)]
                    CONTAINS_KEY_CACHE_HIT_COUNT.with_label_values(&[]).inc();
                    results[i] = value;
                } else {
                    #[cfg(with_metrics)]
                    CONTAINS_KEY_CACHE_MISS_COUNT.with_label_values(&[]).inc();
                    indices.push(i);
                    key_requests.push(keys[i].clone());
                }
            }
        }
        if !key_requests.is_empty() {
            let key_results = self.store.contains_keys(key_requests.clone()).await?;
            let mut cache = cache.lock().unwrap();
            for ((index, result), key) in indices.into_iter().zip(key_results).zip(key_requests) {
                results[index] = result;
                cache.insert_contains_key(key, result);
            }
        }
        Ok(results)
    }

    async fn read_multi_values_bytes(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let Some(cache) = &self.cache else {
            return self.store.read_multi_values_bytes(keys).await;
        };

        let mut result = Vec::with_capacity(keys.len());
        let mut cache_miss_indices = Vec::new();
        let mut miss_keys = Vec::new();
        {
            let cache = cache.lock().unwrap();
            for (i, key) in keys.into_iter().enumerate() {
                if let Some(value) = cache.query_read_value(&key) {
                    #[cfg(with_metrics)]
                    READ_VALUE_CACHE_HIT_COUNT.with_label_values(&[]).inc();
                    result.push(value);
                } else {
                    #[cfg(with_metrics)]
                    READ_VALUE_CACHE_MISS_COUNT.with_label_values(&[]).inc();
                    result.push(None);
                    cache_miss_indices.push(i);
                    miss_keys.push(key);
                }
            }
        }
        if !miss_keys.is_empty() {
            let values = self
                .store
                .read_multi_values_bytes(miss_keys.clone())
                .await?;
            let mut cache = cache.lock().unwrap();
            for (i, (key, value)) in cache_miss_indices
                .into_iter()
                .zip(miss_keys.into_iter().zip(values))
            {
                cache.insert_read_value(key, &value);
                result[i] = value;
            }
        }
        Ok(result)
    }

    async fn find_keys_by_prefix(&self, key_prefix: &[u8]) -> Result<Self::Keys, Self::Error> {
        self.store.find_keys_by_prefix(key_prefix).await
    }

    async fn find_key_values_by_prefix(
        &self,
        key_prefix: &[u8],
    ) -> Result<Self::KeyValues, Self::Error> {
        self.store.find_key_values_by_prefix(key_prefix).await
    }
}

impl<K> WritableKeyValueStore for LruCachingStore<K>
where
    K: WritableKeyValueStore + Send + Sync,
{
    // The LRU cache does not change the underlying store's size limits.
    const MAX_VALUE_SIZE: usize = K::MAX_VALUE_SIZE;

    async fn write_batch(&self, batch: Batch) -> Result<(), Self::Error> {
        let Some(cache) = &self.cache else {
            return self.store.write_batch(batch).await;
        };

        {
            let mut cache = cache.lock().unwrap();
            for operation in &batch.operations {
                match operation {
                    WriteOperation::Put { key, value } => {
                        cache.correct_find_entry(key, Some(value.to_vec()));
                        let cache_entry = ValueCacheEntry::Value(value.to_vec());
                        cache.insert_value(key.to_vec(), cache_entry);
                    }
                    WriteOperation::Delete { key } => {
                        cache.correct_find_entry(key, None);
                        let cache_entry = ValueCacheEntry::DoesNotExist;
                        cache.insert_value(key.to_vec(), cache_entry);
                    }
                    WriteOperation::DeletePrefix { key_prefix } => {
                        cache.delete_prefix(key_prefix);
                    }
                }
            }
        }
        self.store.write_batch(batch).await
    }

    async fn clear_journal(&self) -> Result<(), Self::Error> {
        self.store.clear_journal().await
    }
}

/// The configuration type for the `LruCachingStore`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LruCachingConfig<C> {
    /// The inner configuration of the `LruCachingStore`.
    pub inner_config: C,
    /// The cache size being used
    pub storage_cache_config: StorageCacheConfig,
}

impl<K> AdminKeyValueStore for LruCachingStore<K>
where
    K: AdminKeyValueStore + Send + Sync,
{
    type Config = LruCachingConfig<K::Config>;

    fn get_name() -> String {
        format!("lru caching {}", K::get_name())
    }

    async fn connect(config: &Self::Config, namespace: &str) -> Result<Self, Self::Error> {
        let store = K::connect(&config.inner_config, namespace).await?;
        Ok(LruCachingStore::new(
            store,
            config.storage_cache_config.clone(),
        ))
    }

    fn clone_with_root_key(&self, root_key: &[u8]) -> Result<Self, Self::Error> {
        let store = self.store.clone_with_root_key(root_key)?;
        let store = LruCachingStore::new(store, self.storage_cache_config());
        store.enable_exclusive_access();
        Ok(store)
    }

    async fn list_all(config: &Self::Config) -> Result<Vec<String>, Self::Error> {
        K::list_all(&config.inner_config).await
    }

    async fn list_root_keys(
        config: &Self::Config,
        namespace: &str,
    ) -> Result<Vec<Vec<u8>>, Self::Error> {
        K::list_root_keys(&config.inner_config, namespace).await
    }

    async fn delete_all(config: &Self::Config) -> Result<(), Self::Error> {
        K::delete_all(&config.inner_config).await
    }

    async fn exists(config: &Self::Config, namespace: &str) -> Result<bool, Self::Error> {
        K::exists(&config.inner_config, namespace).await
    }

    async fn create(config: &Self::Config, namespace: &str) -> Result<(), Self::Error> {
        K::create(&config.inner_config, namespace).await
    }

    async fn delete(config: &Self::Config, namespace: &str) -> Result<(), Self::Error> {
        K::delete(&config.inner_config, namespace).await
    }
}

#[cfg(with_testing)]
impl<K> TestKeyValueStore for LruCachingStore<K>
where
    K: TestKeyValueStore + Send + Sync,
{
    async fn new_test_config() -> Result<LruCachingConfig<K::Config>, K::Error> {
        let inner_config = K::new_test_config().await?;
        let storage_cache_config = DEFAULT_STORAGE_CACHE_CONFIG;
        Ok(LruCachingConfig {
            inner_config,
            storage_cache_config,
        })
    }
}

impl<K> LruCachingStore<K> {
    /// Creates a new key-value store that provides LRU caching at top of the given store.
    pub fn new(store: K, storage_cache_config: StorageCacheConfig) -> Self {
        let cache = {
            if storage_cache_config.max_cache_entries == 0 {
                None
            } else {
                Some(Arc::new(Mutex::new(LruPrefixCache::new(
                    storage_cache_config,
                ))))
            }
        };
        Self { store, cache }
    }

    /// Gets the `cache_size`.
    pub fn storage_cache_config(&self) -> StorageCacheConfig {
        match &self.cache {
            None => StorageCacheConfig {
                max_cache_size: 0,
                max_entry_size: 0,
                max_cache_entries: 0,
            },
            Some(cache) => {
                let cache = cache.lock().unwrap();
                cache.storage_cache_config.clone()
            }
        }
    }

    /// Sets the value `has_exclusive_access` to `true`, if applicable.
    pub fn enable_exclusive_access(&self) {
        if let Some(cache) = &self.cache {
            let mut cache = cache.lock().unwrap();
            cache.has_exclusive_access = true;
        }
    }
}

/// A memory store with caching.
#[cfg(with_testing)]
pub type LruCachingMemoryStore = LruCachingStore<MemoryStore>;
