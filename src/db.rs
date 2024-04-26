use crate::tree::{LeafNode, LEAF_NODE_MAX_CELLS};
// module imports
use crate::{cursor, pager};

use cursor::Cursor;
use log::info;

use std::io::{stdin, stdout, Write};
use std::mem;
use std::os::unix::fs::FileExt;
use std::process::exit;

use pager::Pager;
use pager::PAGE_SIZE;

enum StatementType {
    Select,
    Insert,
}

struct Statement {
    statement_type: StatementType,
    row_to_insert: Row,
}

const MAX_STRING_SIZE: usize = 64;
const ID_SIZE: usize = mem::size_of::<u32>();
const USERNAME_SIZE: usize = mem::size_of::<u8>() * MAX_STRING_SIZE;
const EMAIL_SIZE: usize = mem::size_of::<u8>() * MAX_STRING_SIZE;

const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;

pub const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
pub const ROWS_PER_PAGE: u32 = PAGE_SIZE as u32 / ROW_SIZE as u32;

pub struct Table {
    pub root_page_num: u32,
    pub pager: Pager,
}

impl Table {
    fn new(file_descriptor: String) -> Self {
        let pager = Pager::open_file(file_descriptor).unwrap();

        Table {
            root_page_num: 0,
            pager,
        }
    }
}

pub struct Row {
    id: u32,
    username: String,
    email: String,
}

pub struct Db {
    pub table: Table,
}

impl Db {
    pub fn new(file_descriptor: String) -> Db {
        Db {
            table: Table::new(file_descriptor),
        }
    }

    pub fn run_db(&mut self) {
        info!("Initialized QBA-DB version 0.0.1");

        loop {
            print_prompt();
            let mut user_input = String::new();
            let _ = stdout().flush();
            stdin()
                .read_line(&mut user_input)
                .expect("Did not enter a correct string");
            if let Some('\n') = user_input.chars().next_back() {
                user_input.pop();
            }
            if let Some('\r') = user_input.chars().next_back() {
                user_input.pop();
            }

            if user_input.starts_with('.') {
                match perform_meta_command(&user_input) {
                    MetaCommandResponse::Success => {
                        continue;
                    }
                    MetaCommandResponse::UnrecognizedCommand => {
                        info!("Unrecognized command {}", user_input);
                        continue;
                    }
                    MetaCommandResponse::Exit => return,
                }
            }

            // prepare statement
            let mut cur_statement: Statement = Statement {
                statement_type: StatementType::Select,
                row_to_insert: Row {
                    id: 0,
                    username: "".to_string(),
                    email: "".to_string(),
                },
            };

            match prepare_statement(&user_input, &mut cur_statement) {
                StatementPrepareResponse::Success => {
                    execute_statement(cur_statement, &mut self.table);
                }
                StatementPrepareResponse::UnrecognizedCommand => {
                    info!("Unrecognized statement {}", user_input);
                    continue;
                }
                StatementPrepareResponse::SyntaxError => {
                    info!("Syntax error in statement {}", user_input);
                    continue;
                }
            }
        }
    }

    pub fn run_db_test(&mut self, user_input: String) {
        // info!("Executing statement: {}", user_input);

        if user_input.starts_with('.') {
            match perform_meta_command(&user_input) {
                MetaCommandResponse::Success => {}
                MetaCommandResponse::UnrecognizedCommand => {
                    info!("Unrecognized command {}", user_input);
                }
                MetaCommandResponse::Exit => return,
            }
        }

        // prepare statement
        let mut cur_statement: Statement = Statement {
            statement_type: StatementType::Select,
            row_to_insert: Row {
                id: 0,
                username: "".to_string(),
                email: "".to_string(),
            },
        };

        match prepare_statement(&user_input, &mut cur_statement) {
            StatementPrepareResponse::Success => {
                execute_statement(cur_statement, &mut self.table);
            }
            StatementPrepareResponse::UnrecognizedCommand => {
                info!("Unrecognized statement {}", user_input);
            }
            StatementPrepareResponse::SyntaxError => {
                info!("Syntax error in statement {}", user_input);
            }
        }
    }

    pub fn close_db(&mut self) -> Result<(), &str> {
        info!("SHOULD WRITE DATA TO FILE");
        // write all bytes of pages into file;
        let mut cursor = Cursor::table_start(&mut self.table);

        let mut end_of_table = cursor.end_of_table;
        let mut pages_written = 0;

        while end_of_table == false {
            info!("trying to save node {}", pages_written);
            let node = cursor
                .table
                .pager
                .get_page(cursor.page_num as usize)
                .unwrap();

            let mut page_to_write = [0u8; PAGE_SIZE];
            info!("deserializing node to {:?}", page_to_write.as_ptr());
            LeafNode::deserialize_node(node, page_to_write.as_mut_ptr());

            info!("saving node to file");
            match cursor
                .table
                .pager
                .file_descriptor
                .write_all_at(&page_to_write, PAGE_SIZE as u64 * pages_written)
            {
                Ok(_) => {
                    pages_written = pages_written + 1;
                }
                Err(_) => return Err("Error saving db to file!"),
            }

            cursor.advance_cursor();
            end_of_table = cursor.end_of_table;
        }

        // let mut pages_written = 0;
        // let num_full_pages = (self.table.num_rows / ROWS_PER_PAGE) as usize;

        // for i in 0..num_full_pages {
        //     let page_opt = self.table.pager.pages.get(i).unwrap();

        //     match page_opt {
        //         Some(page) => {
        //             // if pages are full write all to disk
        //             match self
        //                 .table
        //                 .pager
        //                 .file_descriptor
        //                 .write_all_at(page, PAGE_SIZE as u64 * pages_written)
        //             {
        //                 Ok(_) => {
        //                     pages_written = pages_written + 1;
        //                 }
        //                 Err(_) => return Err("Error saving db to file!"),
        //             }
        //         }
        //         None => {}
        //     }
        // }

        // // handle partial page at the end
        // let remaining_rows = (self.table.num_rows % ROWS_PER_PAGE) as usize;

        // if remaining_rows > 0 {
        //     let page_opt = self.table.pager.pages.get(num_full_pages).unwrap();

        //     match page_opt {
        //         Some(page) => {
        //             let bytes_to_write = remaining_rows * ROW_SIZE;

        //             match self
        //                 .table
        //                 .pager
        //                 .file_descriptor
        //                 .write_all_at(&page[0..bytes_to_write], PAGE_SIZE as u64 * pages_written)
        //             {
        //                 Ok(_) => {}
        //                 Err(_) => return Err("Error saving db to file!"),
        //             }
        //         }
        //         None => {}
        //     }
        // }

        Ok(())
    }
}

fn print_prompt() {
    print!("qba-db> ");
}

enum MetaCommandResponse {
    Success,
    UnrecognizedCommand,
    Exit,
}

fn perform_meta_command(command: &String) -> MetaCommandResponse {
    if command == ".exit" {
        return MetaCommandResponse::Exit;
    } else if command == ".ping" {
        info!("pong!");
        return MetaCommandResponse::Success;
    } else {
        return MetaCommandResponse::UnrecognizedCommand;
    }
}

enum StatementPrepareResponse {
    Success,
    SyntaxError,
    UnrecognizedCommand,
}

fn prepare_statement(user_input: &String, statement: &mut Statement) -> StatementPrepareResponse {
    if user_input.starts_with("select") {
        statement.statement_type = StatementType::Select;
        return StatementPrepareResponse::Success;
    } else if user_input.starts_with("insert") {
        statement.statement_type = StatementType::Insert;

        // read arguments from user input
        let row_args: Vec<&str> = user_input.split_whitespace().collect();

        if row_args.len() != 4 {
            return StatementPrepareResponse::SyntaxError;
        }

        statement.row_to_insert.id = row_args[1].parse::<u32>().unwrap();
        statement.row_to_insert.username = row_args[2].to_string();
        statement.row_to_insert.email = row_args[3].to_string();

        return StatementPrepareResponse::Success;
    } else {
        return StatementPrepareResponse::UnrecognizedCommand;
    }
}

fn execute_statement(statement: Statement, table: &mut Table) {
    match statement.statement_type {
        StatementType::Select => execute_select_statement(statement, table).unwrap(),
        StatementType::Insert => execute_insert_statement(statement, table).unwrap(),
    }
}

fn execute_select_statement(_: Statement, table: &mut Table) -> Result<(), &'static str> {
    let mut cursor = Cursor::table_start(table);
    let mut end_of_table = cursor.end_of_table;

    while end_of_table == false {
        let row_slot = Cursor::get_cursor_value(&mut cursor).unwrap();

        let mut row_data = Row {
            id: 123,
            email: String::from("123"),
            username: String::from("!@3"),
        };
        deserialize_row(row_slot, &mut row_data).unwrap();

        info!(
            "id: {}, username: {}, email: {}",
            row_data.id, row_data.username, row_data.email
        );

        cursor.advance_cursor();
        end_of_table = cursor.end_of_table;
    }

    Ok(())
}

fn execute_insert_statement(statement: Statement, table: &mut Table) -> Result<(), &'static str> {
    // let num_rows = table.num_rows;
    let row = &statement.row_to_insert;
    let mut cursor = Cursor::table_end(table);
    LeafNode::insert(&mut cursor, row.id, row);

    // // let row_slot = get_table_row(table, num_rows).unwrap();
    // let row_slot = Cursor::get_cursor_value(&mut cursor).unwrap();
    // table.num_rows = num_rows + 1;

    // match serialize_row(&statement.row_to_insert, row_slot) {
    //     Ok(()) => {}
    //     Err(error) => {
    //         table.num_rows = num_rows;
    //         info!("Error inserting row! {}", error);
    //     }
    // }

    Ok(())
}

pub fn serialize_row(source: &Row, destination: *mut u8) -> Result<(), &str> {
    unsafe { return unsafe_serialize_row(source, destination) }
}

pub fn deserialize_row(source: *const u8, destination: &mut Row) -> Result<(), &str> {
    unsafe { return unsafe_deserialize_row(source, destination) }
}

unsafe fn unsafe_serialize_row(source: &Row, destination: *mut u8) -> Result<(), &str> {
    // Serialize ID
    std::ptr::copy_nonoverlapping(
        &source.id as *const _ as *const u8,
        destination.offset(ID_OFFSET as isize),
        ID_SIZE,
    );

    // Serialize Username
    if source.username.len() > MAX_STRING_SIZE {
        return Err("Username is too long!");
    }
    let username_bytes = source.username.as_bytes();
    std::ptr::write_bytes(
        destination.offset(USERNAME_OFFSET as isize),
        0,
        USERNAME_SIZE,
    );
    std::ptr::copy_nonoverlapping(
        username_bytes.as_ptr(),
        destination.offset(USERNAME_OFFSET as isize),
        username_bytes.len(),
    );

    // Serialize Email
    if source.email.len() > MAX_STRING_SIZE {
        return Err("Email is too long!");
    }
    let email_bytes = source.email.as_bytes();
    std::ptr::write_bytes(destination.offset(EMAIL_OFFSET as isize), 0u8, EMAIL_SIZE);
    std::ptr::copy_nonoverlapping(
        email_bytes.as_ptr(),
        destination.offset(EMAIL_OFFSET as isize),
        email_bytes.len(),
    );

    // info!();
    // println!("[EMAIL]: now going to read what we just wrote!");
    // let email_slice =
    //     std::slice::from_raw_parts(destination.offset(EMAIL_OFFSET as isize), EMAIL_SIZE);
    // println!(
    //     "[EMAIL]: Reading bytes: from source {:?} with len {}. bytes are {:?}",
    //     destination.offset(EMAIL_OFFSET as isize),
    //     EMAIL_SIZE,
    //     email_slice
    // );

    Ok(())
}

unsafe fn unsafe_deserialize_row(source: *const u8, destination: &mut Row) -> Result<(), &str> {
    // SAFER: Deserialize ID
    let id_slice = std::slice::from_raw_parts(source.offset(ID_OFFSET as isize), ID_SIZE);
    let id = u32::from_ne_bytes(id_slice.try_into().unwrap());

    // SAFER: Deserialize USERNAME
    let username_slice =
        std::slice::from_raw_parts(source.offset(USERNAME_OFFSET as isize), USERNAME_SIZE);
    let username = std::str::from_utf8(username_slice).unwrap().to_string();

    // SAFER: Deserialize EMAIL
    let email_slice = std::slice::from_raw_parts(source.offset(EMAIL_OFFSET as isize), EMAIL_SIZE);
    let email = std::str::from_utf8(email_slice).unwrap().to_string();

    destination.id = id;
    destination.username = username;
    destination.email = email;

    Ok(())
}
