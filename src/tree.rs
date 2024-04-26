use crate::{
    cursor::Cursor,
    db::{self, serialize_row, Row},
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
const PARENT_POINTER_SIZE: usize = mem::size_of::<*mut u8>();
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

// row size
const CELL_SIZE: usize = ROW_SIZE;

#[derive(Clone)]
pub struct LeafNode {
    pub is_root: bool,
    pub parent_ptr: Option<*mut u8>,
    // leaf_format
    pub num_cells: u32,
    pub cells: [u8; LEAF_NODE_SPACE_FOR_CELLS],
}

impl LeafNode {
    pub fn new() -> LeafNode {
        return LeafNode {
            is_root: false,
            parent_ptr: None,
            num_cells: 0,
            cells: [0; LEAF_NODE_SPACE_FOR_CELLS],
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

    pub fn deserialize_node(node: &mut LeafNode, destination: *mut u8) {
        // write that this node is a leaf node
        unsafe {
            info!("writing node type");
            // write node type
            ptr::write_bytes(
                destination.offset(NODE_TYPE_OFFSET as isize),
                1u8,
                NODE_TYPE_SIZE,
            );

            // pub is_root: bool,
            info!("writing is root");
            ptr::copy_nonoverlapping(
                &node.is_root,
                destination.offset(IS_ROOT_OFFSET as isize) as *mut bool,
                IS_ROOT_SIZE,
            );

            // pub parent_ptr: Option<*mut u8>,
            info!("writing parent_ptr");
            match node.parent_ptr {
                Some(parent_ptr) => ptr::copy_nonoverlapping(
                    &parent_ptr,
                    destination.offset(PARENT_POINTER_OFFSET as isize) as *mut *mut u8,
                    PARENT_POINTER_SIZE,
                ),
                None => ptr::write_bytes(
                    destination.offset(PARENT_POINTER_OFFSET as isize) as *mut *mut u8,
                    0,
                    PARENT_POINTER_SIZE,
                ),
            }

            // pub num_cells: u32,
            info!("writing num_cells");
            ptr::copy_nonoverlapping(
                &node.num_cells as *const _ as *const u8,
                destination.offset(LEAF_NODE_NUM_CELLS_OFFSET as isize) as *mut u8,
                LEAF_NODE_NUM_CELLS_SIZE,
            );

            // pub cells: Vec<u8>,
            info!("writing cells");
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
            // TODO: load parent ptr from memory
            let parent_ptr: Option<*mut u8> = None;

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

    pub fn insert(cursor: &mut Cursor, key: u32, row: &Row) {
        let page_num = cursor.page_num;
        let node = cursor.table.pager.get_page(page_num as usize).unwrap();
        let num_cells = node.num_cells;

        if num_cells as usize >= LEAF_NODE_MAX_CELLS {
            info!("LEAF HAS HIT MAX LIMIT OF CELLS");
            exit(1);
        }

        node.num_cells = num_cells + 1;
        // save key
        unsafe {
            ptr::copy_nonoverlapping(
                &key as *const _ as *const u8,
                node.get_cell_key(cursor.cell_num),
                LEAF_NODE_KEY_SIZE,
            );
        }

        // save row
        serialize_row(row, node.get_cell_value(cursor.cell_num)).unwrap();
    }
}
