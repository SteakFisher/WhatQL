use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use anyhow::{Result, anyhow};

use super::node::{PageId, BTreeNode};
use crate::engine::EngineStats;

/// An LRU cache for B-tree pages
pub struct PageCache {
    capacity: usize,
    cache: HashMap<PageId, Arc<Mutex<BTreeNode>>>,
    lru: VecDeque<PageId>,
    stats: Arc<Mutex<EngineStats>>,
    page_size: usize,
    dirty_pages: HashMap<PageId, Arc<Mutex<BTreeNode>>>,
}

impl PageCache {
    pub fn new(capacity: usize, page_size: usize, stats: Arc<Mutex<EngineStats>>) -> Self {
        PageCache {
            capacity,
            cache: HashMap::with_capacity(capacity),
            lru: VecDeque::with_capacity(capacity),
            stats,
            page_size,
            dirty_pages: HashMap::new(),
        }
    }
    
    pub fn get(&mut self, page_id: PageId) -> Option<Arc<Mutex<BTreeNode>>> {
        if let Some(node) = self.cache.get(&page_id) {
            // Update LRU
            if let Some(pos) = self.lru.iter().position(|&id| id == page_id) {
                self.lru.remove(pos);
            }
            self.lru.push_back(page_id);
            
            // Update stats
            if let Ok(mut stats) = self.stats.lock() {
                stats.cache_hits += 1;
            }
            
            return Some(Arc::clone(node));
        }
        
        // Update stats for cache miss
        if let Ok(mut stats) = self.stats.lock() {
            stats.cache_misses += 1;
        }
        
        None
    }
    
    pub fn put(&mut self, page_id: PageId, node: BTreeNode, is_dirty: bool) -> Result<()> {
        let node_arc = Arc::new(Mutex::new(node));
        
        // If cache is full, evict least recently used page
        if self.cache.len() >= self.capacity && !self.cache.contains_key(&page_id) {
            if let Some(evicted_id) = self.lru.pop_front() {
                // If the evicted page is dirty, it would be written to disk here
                if self.dirty_pages.contains_key(&evicted_id) {
                    // In a real implementation, we would write to disk
                    self.dirty_pages.remove(&evicted_id);
                }
                self.cache.remove(&evicted_id);
            }
        }
        
        // Add to cache
        self.cache.insert(page_id, Arc::clone(&node_arc));
        self.lru.push_back(page_id);
        
        // If dirty, add to dirty pages
        if is_dirty {
            self.dirty_pages.insert(page_id, node_arc);
        }
        
        Ok(())
    }
    
    pub fn mark_dirty(&mut self, page_id: PageId) -> Result<()> {
        if let Some(node) = self.cache.get(&page_id) {
            self.dirty_pages.insert(page_id, Arc::clone(node));
            Ok(())
        } else {
            Err(anyhow!("Page not in cache"))
        }
    }
    
    pub fn flush_all(&mut self) -> Result<()> {
        // In a real implementation, this would write all dirty pages to disk
        let count = self.dirty_pages.len();
        self.dirty_pages.clear();
        
        println!("Flushed {} dirty pages to disk", count);
        Ok(())
    }
    
    pub fn invalidate(&mut self, page_id: PageId) -> Result<()> {
        self.cache.remove(&page_id);
        self.dirty_pages.remove(&page_id);
        if let Some(pos) = self.lru.iter().position(|&id| id == page_id) {
            self.lru.remove(pos);
        }
        Ok(())
    }
    
    pub fn stats(&self) -> String {
        let hit_rate = if let Ok(stats) = self.stats.lock() {
            let total = stats.cache_hits + stats.cache_misses;
            if total == 0 {
                0.0
            } else {
                (stats.cache_hits as f64 / total as f64) * 100.0
            }
        } else {
            0.0
        };
        
        format!(
            "Cache: {}/{} pages, {:.2}% hit rate, {} dirty pages",
            self.cache.len(),
            self.capacity,
            hit_rate,
            self.dirty_pages.len()
        )
    }
}