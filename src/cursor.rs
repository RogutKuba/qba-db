use crate::{pager::TABLE_MAX_PAGES, Table, ROWS_PER_PAGE, ROW_SIZE};

pub struct Cursor<'a> {
    pub table: &'a mut Table,
    pub row_num: u32,
    pub end_of_table: bool,
}

impl<'a> Cursor<'a> {
    pub fn table_start(table: &mut Table) -> Cursor {
        let num_rows = table.num_rows;
        return Cursor {
            table,
            row_num: 0,
            end_of_table: num_rows == 0,
        };
    }

    pub fn table_end(table: &mut Table) -> Cursor {
        let num_rows = table.num_rows;
        return Cursor {
            table,
            row_num: num_rows,
            end_of_table: true,
        };
    }

    pub fn advance_cursor(&mut self) {
        self.row_num = self.row_num + 1;

        if self.row_num >= self.table.num_rows {
            self.end_of_table = true;
        }
    }

    pub fn get_cursor_value(cursor: &mut Cursor) -> Result<*mut u8, &'static str> {
        let row_num = cursor.row_num;
        let page_num: usize = (row_num / ROWS_PER_PAGE) as usize;
        // info!("total pages: {}", table.pages.len());

        if page_num > TABLE_MAX_PAGES as usize {
            return Err("Trying to fetch from page out of bounds!");
        }

        let cur_page = cursor.table.pager.get_page(page_num);

        // info!("fetching from pages {}", page_num);

        match cur_page {
            Ok(page) => unsafe {
                // info!(
                //     "Has page at address: {:?}. Adding {} rows",
                //     page.as_ptr(),
                //     row_num
                // );
                return Ok(page.add(ROW_SIZE * (row_num % ROWS_PER_PAGE) as usize));
            },
            Err(_) => return Err("Error fetching page from table"),
        }
    }
}
