use std::{mem, ptr};

use log::info;

use crate::{
    cursor::Cursor,
    db::Table,
    leaf_node::{
        LeafNode, COMMON_NODE_HEADER_SIZE, IS_ROOT_OFFSET, IS_ROOT_SIZE, NODE_TYPE_OFFSET,
        NODE_TYPE_SIZE, PARENT_POINTER_OFFSET, PARENT_POINTER_SIZE,
    },
    pager::{NodeType, PAGE_SIZE},
};
/*
* Internal Node Header Layout
*/
const INTERNAL_NODE_NUM_KEYS_SIZE: usize = mem::size_of::<u32>();
const INTERNAL_NODE_NUM_KEYS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const INTERNAL_NODE_RIGHT_CHILD_SIZE: usize = mem::size_of::<u32>();
const INTERNAL_NODE_RIGHT_CHILD_OFFSET: usize =
    INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const INTERNAL_NODE_HEADER_SIZE: usize =
    COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;

/*
* Internal Node Body Layout
*/
const INTERNAL_NODE_KEY_SIZE: usize = mem::size_of::<u32>();
const INTERNAL_NODE_CHILD_SIZE: usize = mem::size_of::<u32>();
const INTERNAL_NODE_CELL_SIZE: usize = INTERNAL_NODE_KEY_SIZE + INTERNAL_NODE_CHILD_SIZE;

const INTERNAL_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE;
const INTERNAL_NODE_MAX_CELLS: usize = INTERNAL_NODE_SPACE_FOR_CELLS / INTERNAL_NODE_CELL_SIZE;

#[derive(Clone)]
pub struct InternalNode {
    pub is_root: bool,
    pub parent_ptr: u32,
    // internal_format
    pub num_keys: u32,
    pub right_child: u32,
    pub cells: [(u32, u32); INTERNAL_NODE_MAX_CELLS],
}

impl InternalNode {
    pub fn new() -> InternalNode {
        return InternalNode {
            is_root: false,
            parent_ptr: 0,
            num_keys: 0,
            right_child: 0,
            cells: [(0, 0); INTERNAL_NODE_MAX_CELLS], // stored as (key, page_num)
        };
    }

    pub fn create_new_root_from_leaf(table: &mut Table, right_page_num: u32) {
        /*
         * Old root node is the node we split into old_root & right_node
         * nowe we need to move the data from the old "left" node into a new page
         * and change the root back into a regular root node
         */
        let left_child_page_num = table.pager.get_unused_page_num();
        // info!(
        //     "Creating node through create internal root node method at {}",
        //     left_child_page_num
        // );
        let old_root_node = table
            .pager
            .get_page_leaf(table.root_page_num as usize)
            .unwrap();

        // write to new node
        table.pager.pages[left_child_page_num as usize] =
            (None, Some(Box::new(old_root_node.clone())));

        // let left_child_node = table
        //     .pager
        //     .get_page_leaf(left_child_page_num as usize)
        //     .unwrap();

        let (left_child_node, right_child_node) = table
            .pager
            .get_two_pages_leaf(left_child_page_num as usize, right_page_num as usize)
            .unwrap();

        left_child_node.is_root = false;
        let left_node_max_key = left_child_node.get_max_key();

        left_child_node.parent = table.root_page_num;
        right_child_node.parent = table.root_page_num;

        // make old root page num into internal node
        table.pager.num_pages = table.pager.num_pages + 1;
        table.pager.pages[table.root_page_num as usize] =
            (Some(Box::new(InternalNode::new())), None);
        let new_root_node = table
            .pager
            .get_page_internal(table.root_page_num as usize)
            .unwrap();
        new_root_node.is_root = true;
        new_root_node.num_keys = 1;

        // write child into cell for internal node
        new_root_node.cells[0] = (left_node_max_key, left_child_page_num);
        new_root_node.right_child = right_page_num;
    }

    pub fn update_internal_node_key(&mut self, old_max: u32, new_key: u32) {
        let old_child_index = self.find_child_index(old_max);
        let old_tuple = self.cells[old_child_index as usize];

        self.cells[old_child_index as usize] = (new_key, old_tuple.1);
    }

    pub fn internal_node_insert(table: &mut Table, parent_page_num: usize, child_page_num: usize) {
        let (parent, child, right_child) = table
            .pager
            .get_nodes_for_internal_node_insert(parent_page_num, child_page_num)
            .unwrap();

        let child_max_key = child.get_max_key();
        let child_index = parent.find_child_index(child_max_key);

        let original_num_keys = parent.num_keys;
        parent.num_keys = original_num_keys + 1;

        if original_num_keys as usize >= INTERNAL_NODE_MAX_CELLS {
            panic!("NEED TO IMPLEMENT SPLITTING INTERNAL NODE!!");
        }

        let right_child_page_num = parent.right_child as usize;
        let right_child_max_key = right_child.get_max_key();

        // info!(
        //     "child_max_key: {}, right_child_max_key: {}",
        //     child_max_key, right_child_max_key
        // );

        if child_max_key > right_child_max_key {
            // info!("Have to replace right child in parent internal node! Going to set right_child to {}", child_page_num);
            // replace right child
            parent.cells[original_num_keys as usize] =
                (right_child_max_key, right_child_page_num as u32);
            parent.right_child = child_page_num as u32;

            // info!(
            //     "setting index {} to {:?}",
            //     original_num_keys,
            //     (right_child_max_key, right_child_page_num as u32)
            // );
        } else {
            // info!("looping from {}..={}.rev()", child_index, original_num_keys);

            // make room for new cell
            for i in (child_index..=original_num_keys).rev() {
                // info!("setting index {} = {:?}", i, parent.cells[i as usize - 1]);
                parent.cells[i as usize] = parent.cells[i as usize - 1];
            }
            parent.cells[child_index as usize] = (child_max_key, child_page_num as u32);
            // info!(
            //     "adding new node: setting index {} = {:?}",
            //     child_index,
            //     (child_max_key, child_page_num as u32)
            // );
        }
    }

    pub fn get_child(&self, child_num: u32) -> u32 {
        let num_keys = self.num_keys;
        if child_num > num_keys {
            panic!("Trying to access child outside of internal node bounds! child_num: {} > num_keys: {}", child_num, num_keys);
        } else if child_num == num_keys {
            return self.right_child;
        }

        self.cells[child_num as usize].1
    }

    pub fn node_find(table: &mut Table, page_num: u32, key: u32) -> Cursor {
        let node = table.pager.get_page_internal(page_num as usize).unwrap();

        let child_index = node.find_child_index(key);
        let child_page_num = node.get_child(child_index);

        match table.pager.get_page_node_type(child_page_num as usize) {
            NodeType::Internal => {
                info!("Next node is internal");
                return InternalNode::node_find(table, child_page_num, key);
            }
            NodeType::Leaf => {
                info!("Next node is leaf");
                return LeafNode::node_find(table, child_page_num, key);
            }
        }
    }

    fn find_child_index(&self, key: u32) -> u32 {
        // perform binary search on keys to find child index
        let mut min_index = 0;
        let mut max_index = self.num_keys;

        while min_index != max_index {
            let index = (min_index + max_index) / 2;
            let key_to_right = self.cells[index as usize].0;

            if key_to_right >= key {
                max_index = index;
            } else {
                min_index = index + 1;
            }
        }

        min_index
    }

    pub fn deserialize_node(node: &mut InternalNode, destination: *mut u8) {
        unsafe {
            // write node type
            ptr::write_bytes(
                destination.offset(NODE_TYPE_OFFSET as isize),
                0u8,
                NODE_TYPE_SIZE,
            );

            // pub is_root: bool,
            ptr::copy_nonoverlapping(
                &node.is_root,
                destination.offset(IS_ROOT_OFFSET as isize) as *mut bool,
                IS_ROOT_SIZE,
            );

            // pub parent_ptr: u32
            ptr::copy_nonoverlapping(
                &node.parent_ptr as *const _ as *const u8,
                destination.offset(PARENT_POINTER_OFFSET as isize) as *mut u8,
                PARENT_POINTER_SIZE,
            );

            // pub num_keys: u32,
            ptr::copy_nonoverlapping(
                &node.num_keys as *const _ as *const u8,
                destination.offset(INTERNAL_NODE_NUM_KEYS_OFFSET as isize) as *mut u8,
                INTERNAL_NODE_NUM_KEYS_SIZE,
            );

            // pub right_child: u32
            ptr::copy_nonoverlapping(
                &node.right_child as *const _ as *const u8,
                destination.offset(INTERNAL_NODE_RIGHT_CHILD_OFFSET as isize) as *mut u8,
                INTERNAL_NODE_RIGHT_CHILD_SIZE,
            );

            // pub cells: Vec<u8>,
            ptr::copy_nonoverlapping(
                &node.cells as *const _ as *const u8,
                destination.offset(INTERNAL_NODE_HEADER_SIZE as isize) as *mut u8,
                INTERNAL_NODE_SPACE_FOR_CELLS,
            );
        }
    }

    pub fn serialize_node(source: *mut u8, dest: &mut InternalNode) {
        unsafe {
            let node_type_slice = std::slice::from_raw_parts(
                source.offset(NODE_TYPE_OFFSET as isize),
                NODE_TYPE_SIZE,
            );
            match node_type_slice.get(0) {
                Some(&1) => panic!("Tried to deserialize leaf node into internal node!"),
                Some(&0) => {}
                _ => panic!("Invalid boolean value"),
            };

            // deserialize is_root
            let is_root_slice =
                std::slice::from_raw_parts(source.offset(IS_ROOT_OFFSET as isize), IS_ROOT_SIZE);
            let is_root = match is_root_slice.get(0) {
                Some(&0) => false,
                Some(&1) => true,
                _ => panic!("Invalid boolean value"),
            };

            // pub parent_ptr: Option<*mut u8>,
            let parent_ptr_slice = std::slice::from_raw_parts(
                source.offset(PARENT_POINTER_OFFSET as isize),
                PARENT_POINTER_SIZE,
            );
            let parent_ptr = u32::from_ne_bytes(parent_ptr_slice.try_into().unwrap());

            // pub num_keys: u32,
            let num_keys_slice = std::slice::from_raw_parts(
                source.offset(INTERNAL_NODE_NUM_KEYS_OFFSET as isize),
                INTERNAL_NODE_NUM_KEYS_SIZE,
            );
            let num_keys = u32::from_ne_bytes(num_keys_slice.try_into().unwrap());

            // pub right_child: u32
            let right_child_slice = std::slice::from_raw_parts(
                source.offset(INTERNAL_NODE_NUM_KEYS_SIZE as isize),
                INTERNAL_NODE_RIGHT_CHILD_SIZE,
            );
            let right_child = u32::from_ne_bytes(right_child_slice.try_into().unwrap());

            // pub cells: Vec<u8>,
            let cells_slice = std::slice::from_raw_parts::<(u32, u32)>(
                source.offset(INTERNAL_NODE_HEADER_SIZE as isize) as *mut (u32, u32),
                INTERNAL_NODE_SPACE_FOR_CELLS,
            );
            let cells: [(u32, u32); INTERNAL_NODE_MAX_CELLS] = cells_slice.try_into().unwrap();

            dest.is_root = is_root;
            dest.parent_ptr = parent_ptr;
            dest.num_keys = num_keys;
            dest.right_child = right_child;
            dest.cells = cells;
        }
    }
}
