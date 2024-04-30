use crate::db::Table;

pub struct InternalNode {
    pub is_root: bool,
    pub parent_ptr: u32,
    // leaf_format
    pub num_cells: u32,
    pub cells: [u8; 20],
}

impl InternalNode {
    pub fn create_new_root(table: &mut Table, right_page_num: u32) {
        /*
         * Old root node is the node we split into old_root & right_node
         * nowe we need to move the data from the old "left" node into a new page
         * and change the root back into a regular root node
         */
        let old_root_node = table.pager.get_page(table.root_page_num as usize).unwrap();
        let right_child_node = table.pager.get_page(right_page_num as usize).unwrap();

        let left_child_page_num = table.pager.get_unused_page_num();
        let left_child_node = table.pager.get_page(left_child_page_num as usize).unwrap();

        // write old node data into left child
        unsafe {
            // write into temp buffer and then write to left_child node
            let tmp_buffer = [0u8; ];
        }
    }
}
