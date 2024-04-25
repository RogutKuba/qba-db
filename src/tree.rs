use crate::{
    cursor::Cursor,
    db::{self, serialize_row, Row},
    pager::PAGE_SIZE,
};
use std::{mem, process::exit};

use db::ROW_SIZE;
use log::info;

// enum NodeType {
//     Root,
//     Leaf,
// }

/*
* Common Node Header Layout
*/
const NODE_TYPE_SIZE: usize = mem::size_of::<u8>();
// const NODE_TYPE_OFFSET: usize = 0;
const IS_ROOT_SIZE: usize = mem::size_of::<u8>();
// const IS_ROOT_OFFSET: usize = NODE_TYPE_SIZE;
const PARENT_POINTER_SIZE: usize = mem::size_of::<u32>();
// const PARENT_POINTER_OFFSET: usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/**
 * Lead Node Header Layout
 */
const LEAF_NODE_NUM_CELLS_SIZE: usize = mem::size_of::<u32>();
// const LEAF_NODE_NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

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

// row size
const CELL_SIZE: usize = ROW_SIZE;

#[derive(Clone)]
pub struct LeafNode {
    pub is_root: bool,
    pub parent_ptr: Option<*mut u8>,
    // leaf_format
    pub num_cells: u32,
    pub cells: Vec<u8>,
}

impl LeafNode {
    pub fn new() -> LeafNode {
        return LeafNode {
            is_root: false,
            parent_ptr: None,
            num_cells: 0,
            cells: vec![0; LEAF_NODE_SPACE_FOR_CELLS],
        };
    }

    fn get_cell(&mut self, cell_num: u32) -> *mut u8 {
        unsafe { self.cells.as_mut_ptr().add(cell_num as usize * CELL_SIZE) }
    }

    pub fn get_cell_key(&mut self, cell_num: u32) -> *mut u8 {
        unsafe { self.get_cell(cell_num).add(LEAF_NODE_KEY_OFFSET) }
    }

    pub fn get_cell_value(&mut self, cell_num: u32) -> *mut u8 {
        unsafe { self.get_cell(cell_num).add(LEAF_NODE_VALUE_OFFSET) }
    }

    pub fn insert(cursor: &mut Cursor, key: u32, row: &Row) {
        let page_num = cursor.page_num;
        let mut node = cursor.table.pager.get_page(page_num as usize).unwrap();
        let num_cells = node.num_cells;

        if num_cells as usize >= LEAF_NODE_MAX_CELLS {
            info!("LEAF HAS HIT MAX LIMIT OF CELLS");
            exit(1);
        }

        node.num_cells = num_cells + 1;
        // save key

        // save row
        serialize_row(row, node.get_cell_value(cursor.cell_num)).unwrap();
    }
}
