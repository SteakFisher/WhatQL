use anyhow::{Result, anyhow};
use std::cmp::Ordering;

use super::node::{BTreeNode, PageId, BTreePageCollection};
use super::BTreeNodeType;

/// Traversal context for navigating the B-tree
pub struct TraversalContext {
    pub current_page: PageId,
    pub depth: usize,
    pub path: Vec<PageId>,
    pub comparisons: usize,
    pub nodes_visited: usize,
}

impl TraversalContext {
    pub fn new(root_page: PageId) -> Self {
        TraversalContext {
            current_page: root_page,
            depth: 0,
            path: vec![root_page],
            comparisons: 0,
            nodes_visited: 1,
        }
    }
}

/// Iterator for walking through all leaf nodes in order
pub struct BTreeIterator {
    page_collection: BTreePageCollection,
    current_leaf: Option<BTreeNode>,
    current_cell_index: usize,
    traversal: TraversalContext,
}

impl BTreeIterator {
    pub fn new(page_collection: BTreePageCollection, root_page: PageId) -> Result<Self> {
        let traversal = TraversalContext::new(root_page);
        let current_leaf = Self::find_leftmost_leaf(&page_collection, root_page)?;
        
        Ok(BTreeIterator {
            page_collection,
            current_leaf: Some(current_leaf),
            current_cell_index: 0,
            traversal,
        })
    }
    
    fn find_leftmost_leaf(page_collection: &BTreePageCollection, start_page: PageId) -> Result<BTreeNode> {
        let mut current_page_id = start_page;
        
        loop {
            let node = page_collection.get_node(current_page_id)?;
            
            match node.header.node_type {
                BTreeNodeType::Leaf => return Ok(node),
                BTreeNodeType::Internal => {
                    // In a real implementation, we'd navigate to the leftmost child
                    // For simplicity, we'll just pretend the node is already a leaf
                    return Ok(BTreeNode::new(current_page_id, BTreeNodeType::Leaf, 4096));
                },
                _ => return Err(anyhow!("Unexpected node type during traversal"))
            }
        }
    }
    
    pub fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if let Some(leaf) = &self.current_leaf {
            if self.current_cell_index < leaf.header.cell_count as usize {
                // In a real implementation, we'd extract the actual key-value pair
                // Here we'll just create dummy data
                let key = vec![self.current_cell_index as u8, 0, 0, 0];
                let value = vec![42, 42, 42, 42];
                
                self.current_cell_index += 1;
                return Some((key, value));
            } else {
                // We've exhausted this leaf node, move to the next one
                // In reality, we'd follow the "next leaf" pointer
                // Here we'll just end the iteration
                self.current_leaf = None;
            }
        }
        
        None
    }
}

/// Utility functions for B-tree traversal
pub struct BTreeTraversal;

impl BTreeTraversal {
    pub fn search(pages: &BTreePageCollection, root_page: PageId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut context = TraversalContext::new(root_page);
        
        loop {
            let node = pages.get_node(context.current_page)?;
            context.nodes_visited += 1;
            
            match node.header.node_type {
                BTreeNodeType::Leaf => {
                    // In a real implementation, we'd search for the key in the leaf node
                    // Here we'll just pretend we've found it
                    return Ok(Some(vec![1, 2, 3, 4, 5]));
                },
                BTreeNodeType::Internal => {
                    // In a real implementation, we'd navigate to the appropriate child node
                    // Here we'll just pretend we've reached a leaf already
                    let dummy_leaf_id = PageId(context.current_page.0 + 1);
                    context.current_page = dummy_leaf_id;
                    context.path.push(dummy_leaf_id);
                    context.depth += 1;
                },
                _ => return Err(anyhow!("Unexpected node type during search"))
            }
        }
    }
    
    pub fn calculate_fan_out(node: &BTreeNode) -> usize {
        // Calculate the maximum number of keys per page (fan-out)
        match node.header.node_type {
            BTreeNodeType::Internal => {
                // For internal nodes, factor in key size and child pointers
                let typical_key_size = 8;  // Assuming 8-byte keys on average
                let pointer_size = std::mem::size_of::<PageId>();
                
                let usable_space = node.data.len() - super::BTREE_HEADER_SIZE;
                usable_space / (typical_key_size + pointer_size)
            },
            BTreeNodeType::Leaf => {
                // For leaf nodes, consider average key-value pair size
                let typical_pair_size = 32;  // Assuming 32-byte key-value pairs on average
                
                let usable_space = node.data.len() - super::BTREE_HEADER_SIZE;
                usable_space / typical_pair_size
            },
            _ => 0  // Other page types don't have a meaningful fan-out
        }
    }
    
    pub fn compare_keys(a: &[u8], b: &[u8]) -> Ordering {
        // Binary comparison of keys
        for (a_byte, b_byte) in a.iter().zip(b.iter()) {
            match a_byte.cmp(b_byte) {
                Ordering::Equal => continue,
                non_equal => return non_equal,
            }
        }
        
        // If we get here, one key is a prefix of the other or they're equal
        a.len().cmp(&b.len())
    }
}