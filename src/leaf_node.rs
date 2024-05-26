use crate::{
    cursor::Cursor,
    db::{self, serialize_row, Row, Table},
    internal_node::InternalNode,
    pager::PAGE_SIZE,
};
use std::{mem, ptr};

use db::ROW_SIZE;
use log::info;

// enum NodeType {
//     Root,
//     Leaf,
// }

/*
* Common Node Header Layout
*/
pub const NODE_TYPE_SIZE: usize = mem::size_of::<u8>();
pub const NODE_TYPE_OFFSET: usize = 0;
pub const IS_ROOT_SIZE: usize = mem::size_of::<u8>();
pub const IS_ROOT_OFFSET: usize = NODE_TYPE_SIZE;
pub const PARENT_POINTER_SIZE: usize = mem::size_of::<u32>();
pub const PARENT_POINTER_OFFSET: usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;
pub const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/**
 * Lead Node Header Layout
 */
const LEAF_NODE_NUM_CELLS_SIZE: usize = mem::size_of::<u32>();
const LEAF_NODE_NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const LEAF_NODE_NEXT_LEAF_SIZE: usize = mem::size_of::<u32>();
const LEAF_NODE_NEXT_LEAF_OFFSET: usize = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const LEAF_NODE_HEADER_SIZE: usize =
    COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE;

/**
 * Leaf Node Body Layout
 */
const LEAF_NODE_KEY_SIZE: usize = std::mem::size_of::<u32>();
const LEAF_NODE_KEY_OFFSET: usize = 0;
const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
const LEAF_NODE_VALUE_OFFSET: usize = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
pub const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

/**
 * For splitting
 */
const LEAF_NODE_RIGHT_SPLIT_COUNT: usize = (LEAF_NODE_MAX_CELLS + 1) / 2;
const LEAF_NODE_LEFT_SPLIT_COUNT: usize = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

#[derive(Clone)]
pub struct LeafNode {
    pub is_root: bool,
    pub parent_ptr: u32,
    // leaf_format
    pub num_cells: u32,
    pub next_leaf: u32,
    pub cells: [u8; LEAF_NODE_SPACE_FOR_CELLS],
}

impl LeafNode {
    pub fn new() -> LeafNode {
        return LeafNode {
            is_root: false,
            parent_ptr: 0,
            next_leaf: 0,
            num_cells: 0,
            cells: [0; LEAF_NODE_SPACE_FOR_CELLS],
        };
    }

    fn get_cell(&mut self, cell_num: u32) -> *mut u8 {
        unsafe {
            self.cells
                .as_mut_ptr()
                .add(cell_num as usize * LEAF_NODE_CELL_SIZE)
        }
    }

    pub fn get_cell_key(&mut self, cell_num: u32) -> u32 {
        unsafe {
            let key_slice = std::slice::from_raw_parts(
                self.get_cell(cell_num).add(LEAF_NODE_KEY_OFFSET),
                LEAF_NODE_NUM_CELLS_SIZE,
            );
            u32::from_ne_bytes(key_slice.try_into().unwrap())
        }
    }

    pub fn get_cell_value(&mut self, cell_num: u32) -> *mut u8 {
        unsafe { self.get_cell(cell_num).add(LEAF_NODE_VALUE_OFFSET) }
    }

    pub fn deserialize_node(node: &mut LeafNode, destination: *mut u8) {
        // write that this node is a leaf node
        unsafe {
            // write node type
            ptr::write_bytes(
                destination.offset(NODE_TYPE_OFFSET as isize),
                1u8,
                NODE_TYPE_SIZE,
            );

            // pub is_root: bool,
            // info!("writing is root");
            ptr::copy_nonoverlapping(
                &node.is_root,
                destination.offset(IS_ROOT_OFFSET as isize) as *mut bool,
                IS_ROOT_SIZE,
            );

            // pub parent_ptr: Option<*mut u8>,
            // info!("writing parent_ptr");
            ptr::copy_nonoverlapping(
                &node.parent_ptr as *const _ as *const u8,
                destination.offset(PARENT_POINTER_OFFSET as isize) as *mut u8,
                PARENT_POINTER_SIZE,
            );

            // pub num_cells: u32,
            // info!("writing num_cells");
            ptr::copy_nonoverlapping(
                &node.num_cells as *const _ as *const u8,
                destination.offset(LEAF_NODE_NUM_CELLS_OFFSET as isize) as *mut u8,
                LEAF_NODE_NUM_CELLS_SIZE,
            );

            ptr::copy_nonoverlapping(
                &node.next_leaf as *const _ as *const u8,
                destination.offset(LEAF_NODE_NEXT_LEAF_OFFSET as isize) as *mut u8,
                LEAF_NODE_NEXT_LEAF_SIZE,
            );

            // pub cells: Vec<u8>,
            // info!("writing cells");
            ptr::copy_nonoverlapping(
                &node.cells as *const _ as *const u8,
                destination.offset(LEAF_NODE_HEADER_SIZE as isize) as *mut u8,
                LEAF_NODE_SPACE_FOR_CELLS,
            );
        }
    }

    pub fn serialize_node(source: *mut u8, dest: &mut LeafNode) {
        unsafe {
            let node_type_slice = std::slice::from_raw_parts(
                source.offset(NODE_TYPE_OFFSET as isize),
                NODE_TYPE_SIZE,
            );
            match node_type_slice.get(0) {
                Some(&0) => panic!("Tried to deserialize internal node into leaf node!"),
                Some(&1) => {}
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

            // pub num_cells: u32,
            let num_cells_slice = std::slice::from_raw_parts(
                source.offset(LEAF_NODE_NUM_CELLS_OFFSET as isize),
                LEAF_NODE_NUM_CELLS_SIZE,
            );
            let num_cells = u32::from_ne_bytes(num_cells_slice.try_into().unwrap());

            // pub next_leaf: u32
            let next_leaf_slice = std::slice::from_raw_parts(
                source.offset(LEAF_NODE_NEXT_LEAF_OFFSET as isize),
                LEAF_NODE_NEXT_LEAF_SIZE,
            );
            let next_leaf = u32::from_ne_bytes(next_leaf_slice.try_into().unwrap());

            // pub cells: Vec<u8>,
            let cells_slice = std::slice::from_raw_parts(
                source.offset(LEAF_NODE_HEADER_SIZE as isize),
                LEAF_NODE_SPACE_FOR_CELLS,
            );
            let cells: [u8; LEAF_NODE_SPACE_FOR_CELLS] = cells_slice.try_into().unwrap();

            dest.is_root = is_root;
            dest.parent_ptr = parent_ptr;
            dest.num_cells = num_cells;
            dest.next_leaf = next_leaf;
            dest.cells = cells;
        }
    }

    pub fn node_find(table: &mut Table, page_num: u32, key: u32) -> Cursor {
        let node = table.pager.get_page_leaf(page_num as usize).unwrap();
        let num_cells = node.num_cells;
        let cell_num: u32 = {
            let mut min_index = 0;
            let mut max_index = num_cells;

            while min_index < max_index {
                let index = (min_index + max_index) / 2;

                let key_at_index = node.get_cell_key(index);

                if key == key_at_index {
                    break;
                }

                if key < key_at_index {
                    max_index = index;
                } else {
                    min_index = index + 1;
                }
            }

            (min_index + max_index) / 2
        };
        let end_of_table = node.num_cells == cell_num;

        Cursor {
            table,
            page_num,
            cell_num,
            end_of_table,
        }
    }

    pub fn insert(cursor: &mut Cursor, key: u32, row: &Row) {
        let requires_split = LeafNode::requires_split_and_insert(cursor);

        if requires_split {
            return LeafNode::split_and_insert(cursor, key, row);
        }

        let page_num = cursor.page_num as usize;
        let node = cursor.table.pager.get_page_leaf(page_num).unwrap();
        let num_cells = node.num_cells;

        if cursor.cell_num < num_cells {
            // make room for new cell
            for i in (cursor.cell_num + 1..=num_cells).rev() {
                unsafe {
                    ptr::copy_nonoverlapping(
                        node.get_cell(i - 1) as *mut u8,
                        node.get_cell(i) as *mut u8,
                        LEAF_NODE_CELL_SIZE,
                    );
                }
            }
        }

        node.num_cells = num_cells + 1;
        // save key
        unsafe {
            ptr::copy_nonoverlapping(
                &key as *const _ as *const u8,
                node.get_cell(cursor.cell_num).add(LEAF_NODE_KEY_OFFSET),
                LEAF_NODE_KEY_SIZE,
            );
        }

        // save row
        match serialize_row(row, node.get_cell_value(cursor.cell_num)) {
            Ok(_) => {}
            Err(e) => info!("Could not insert row! {}", e),
        }
    }

    fn requires_split_and_insert(cursor: &mut Cursor) -> bool {
        let page_num = cursor.page_num as usize;
        let node = cursor.table.pager.get_page_leaf(page_num).unwrap();
        let num_cells = node.num_cells;

        return num_cells as usize >= LEAF_NODE_MAX_CELLS;
    }

    fn split_and_insert(cursor: &mut Cursor, key: u32, row: &Row) {
        let pager = &mut cursor.table.pager;

        // Get old_node page first and store necessary info, if required
        let old_page_num = cursor.page_num as usize;
        let new_page_num = pager.get_unused_page_num() as usize;

        // ensure both pages exist
        pager.ensure_page_leaf(old_page_num).unwrap();
        pager.ensure_page_leaf(new_page_num).unwrap();

        if old_page_num >= new_page_num {
            panic!("old page num is greater than new page num!");
        }

        let (mut old_node, mut new_node) = pager
            .get_two_pages_leaf(old_page_num, new_page_num)
            .unwrap();

        // start from right side of leaf node and move cells over to new node
        for i in (0..=LEAF_NODE_MAX_CELLS).rev() {
            //
            let destination_node = {
                if i >= LEAF_NODE_LEFT_SPLIT_COUNT {
                    &mut new_node
                } else {
                    &mut old_node
                }
            };

            let index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
            let destination = destination_node.get_cell(index_within_node as u32);

            if i == cursor.cell_num as usize {
                // save to cell
                unsafe {
                    ptr::copy_nonoverlapping(
                        &key as *const _ as *const u8,
                        destination.add(LEAF_NODE_KEY_OFFSET),
                        LEAF_NODE_KEY_SIZE,
                    );
                    serialize_row(row, destination.add(LEAF_NODE_VALUE_OFFSET)).unwrap();
                }
            } else {
                let cell_to_move = {
                    if i > cursor.cell_num as usize {
                        old_node.get_cell(i as u32 - 1)
                    } else {
                        old_node.get_cell(i as u32)
                    }
                };

                unsafe {
                    ptr::copy_nonoverlapping(cell_to_move, destination, LEAF_NODE_CELL_SIZE);
                }
            }
        }

        old_node.num_cells = LEAF_NODE_LEFT_SPLIT_COUNT as u32;
        new_node.num_cells = LEAF_NODE_RIGHT_SPLIT_COUNT as u32;

        new_node.next_leaf = old_node.next_leaf;
        old_node.next_leaf = new_page_num as u32;

        if old_node.is_root {
            return InternalNode::create_new_root_from_leaf(cursor.table, new_page_num as u32);
        } else {
            // TODO:
            info!("Need to implement setting parent after leafnode split");
        }
    }

    pub fn get_max_key(&mut self) -> u32 {
        self.get_cell_key(self.num_cells - 1)
    }

    pub fn print_node(&mut self) {
        let num_cells = self.num_cells;
        info!("- leaf (num_cells: {})", num_cells);

        for i in 0..num_cells {
            let cell_key = self.get_cell_key(i);
            info!("- {}", cell_key);
        }
    }
}
