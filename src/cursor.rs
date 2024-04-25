use crate::{db, pager};
use db::{Table, ROWS_PER_PAGE, ROW_SIZE};
use log::info;
use pager::TABLE_MAX_PAGES;

pub struct Cursor<'a> {
    pub table: &'a mut Table,
    pub page_num: u32,
    pub cell_num: u32,
    pub end_of_table: bool,
}

impl<'a> Cursor<'a> {
    pub fn table_start(table: &mut Table) -> Cursor {
        let root_page_num = table.root_page_num;
        let root_node = table.pager.get_page(root_page_num as usize).unwrap();
        let num_cells = root_node.num_cells;

        return Cursor {
            table,
            page_num: root_page_num,
            cell_num: 0,
            end_of_table: num_cells == 0,
        };
    }

    pub fn table_end(table: &mut Table) -> Cursor {
        let root_page_num = table.root_page_num;
        let root_node = table.pager.get_page(root_page_num as usize).unwrap();
        let num_cells = root_node.num_cells;

        return Cursor {
            table,
            page_num: root_page_num,
            cell_num: num_cells,
            end_of_table: true,
        };
    }

    pub fn advance_cursor(&mut self) {
        let page_num = self.page_num;
        self.cell_num = self.cell_num + 1;

        let node = self.table.pager.get_page(page_num as usize).unwrap();
        if self.cell_num >= node.num_cells {
            self.end_of_table = true;
        }
    }

    pub fn get_cursor_value(cursor: &mut Cursor) -> Result<*mut u8, &'static str> {
        let page_num = cursor.page_num;
        let node = cursor.table.pager.get_page(page_num as usize).unwrap();
        return Ok(node.get_cell_value(cursor.cell_num));
        // let row_num = cursor.row_num;
        // let page_num: usize = (row_num / ROWS_PER_PAGE) as usize;
        // // info!("total pages: {}", table.pages.len());

        // if page_num > TABLE_MAX_PAGES as usize {
        //     return Err("Trying to fetch from page out of bounds!");
        // }

        // let cur_page = cursor.table.pager.get_page(page_num);

        // // info!("fetching from pages {}", page_num);

        // match cur_page {
        //     Ok(page) => unsafe {
        //         // info!(
        //         //     "Has page at address: {:?}. Adding {} rows",
        //         //     page.as_ptr(),
        //         //     row_num
        //         // );
        //         return Ok(page.add(ROW_SIZE * (row_num % ROWS_PER_PAGE) as usize));
        //     },
        //     Err(_) => return Err("Error fetching page from table"),
        // }
    }
}
