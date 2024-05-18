use crate::{db, internal_node::InternalNode, leaf_node::LeafNode, pager::NodeType};
use db::Table;

pub struct Cursor<'a> {
    pub table: &'a mut Table,
    pub page_num: u32,
    pub cell_num: u32,
    pub end_of_table: bool,
}

impl<'a> Cursor<'a> {
    pub fn table_start(table: &mut Table) -> Cursor {
        let root_page_num = table.root_page_num;

        let num_cells = {
            match table.pager.get_page_node_type(root_page_num as usize) {
                NodeType::Internal => {
                    1 // internal node should never be end of table
                }
                NodeType::Leaf => {
                    let root_node = table.pager.get_page_leaf(root_page_num as usize).unwrap();
                    root_node.num_cells
                }
            }
        };

        return Cursor {
            table,
            page_num: root_page_num,
            cell_num: 0,
            end_of_table: num_cells == 0,
        };
    }

    pub fn table_end(table: &mut Table) -> Cursor {
        let root_page_num = table.root_page_num;
        let root_node = table.pager.get_page_leaf(root_page_num as usize).unwrap();
        let num_cells = root_node.num_cells;

        return Cursor {
            table,
            page_num: root_page_num,
            cell_num: num_cells,
            end_of_table: true,
        };
    }

    pub fn table_find(table: &mut Table, key: u32) -> Cursor {
        let root_page_num = table.root_page_num as usize;

        match table.pager.get_page_node_type(root_page_num) {
            NodeType::Leaf => {
                return LeafNode::node_find(table, root_page_num as u32, key);
            }
            NodeType::Internal => {
                return InternalNode::node_find(table, root_page_num as u32, key);
            }
        }
    }

    pub fn advance_cursor(&mut self) {
        let page_num = self.page_num;
        self.cell_num = self.cell_num + 1;

        let node = self.table.pager.get_page_leaf(page_num as usize).unwrap();
        if self.cell_num >= node.num_cells {
            self.end_of_table = true;
        }
    }

    pub fn get_cursor_value(cursor: &mut Cursor) -> Result<*mut u8, &'static str> {
        let page_num = cursor.page_num as usize;

        match cursor.table.pager.get_page_node_type(page_num) {
            NodeType::Leaf => {
                let node = cursor.table.pager.get_page_leaf(page_num).unwrap();
                return Ok(node.get_cell_value(cursor.cell_num));
            }
            NodeType::Internal => {
                panic!("Trying to fetch value of an internal node");
                // let node = cursor.table.pager.get_page_internal(page_num).unwrap();
                // return Ok(node.get_cell_value(cursor.cell_num));
            }
        }
    }
}
