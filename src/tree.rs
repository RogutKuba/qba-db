use crate::{
    cursor::Cursor,
    db::{self, serialize_row, Row, Table},
    pager::PAGE_SIZE,
};
use std::{mem, process::exit, ptr};

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
const NODE_TYPE_OFFSET: usize = 0;
const IS_ROOT_SIZE: usize = mem::size_of::<u8>();
const IS_ROOT_OFFSET: usize = NODE_TYPE_SIZE;
const PARENT_POINTER_SIZE: usize = mem::size_of::<u32>();
const PARENT_POINTER_OFFSET: usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/**
 * Lead Node Header Layout
 */
const LEAF_NODE_NUM_CELLS_SIZE: usize = mem::size_of::<u32>();
const LEAF_NODE_NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
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

#[derive(Clone)]
pub struct LeafNode {
    pub is_root: bool,
    pub parent_ptr: u32,
    // leaf_format
    pub num_cells: u32,
    pub cells: [u8; LEAF_NODE_SPACE_FOR_CELLS],
}

impl LeafNode {
    pub fn new() -> LeafNode {
        return LeafNode {
            is_root: false,
            parent_ptr: 0,
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

            // pub cells: Vec<u8>,
            let cells_slice = std::slice::from_raw_parts(
                source.offset(LEAF_NODE_HEADER_SIZE as isize),
                LEAF_NODE_SPACE_FOR_CELLS,
            );
            let cells: [u8; LEAF_NODE_SPACE_FOR_CELLS] = cells_slice.try_into().unwrap();

            dest.is_root = is_root;
            dest.parent_ptr = parent_ptr;
            dest.num_cells = num_cells;
            dest.cells = cells;
        }
    }

    pub fn node_find(table: &mut Table, page_num: u32, key: u32) -> Cursor {
        let node = table.pager.get_page(page_num as usize).unwrap();
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
        let page_num = cursor.page_num;
        let node = cursor.table.pager.get_page(page_num as usize).unwrap();
        let num_cells = node.num_cells;

        if num_cells as usize >= LEAF_NODE_MAX_CELLS {
            info!("LEAF HAS HIT MAX LIMIT OF CELLS");
            exit(1);
        }

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
}
