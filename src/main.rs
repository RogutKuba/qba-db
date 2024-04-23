use std::char::MAX;
use std::mem;
use std::{
    io::{stdin, stdout, Write},
    process::exit,
};

enum StatementType {
    Select,
    Insert,
}

struct Statement {
    statement_type: StatementType,
    row_to_insert: Row,
}

const MAX_STRING_SIZE: usize = 64 - 1;
const ID_SIZE: usize = mem::size_of::<u32>();
const USERNAME_SIZE: usize = mem::size_of::<u8>() * MAX_STRING_SIZE;
const EMAIL_SIZE: usize = mem::size_of::<u8>() * MAX_STRING_SIZE;

const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;

const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const PAGE_SIZE: u32 = 4096;
const TABLE_MAX_PAGES: u32 = 100;
const ROWS_PER_PAGE: u32 = PAGE_SIZE / ROW_SIZE as u32;

struct Table {
    num_rows: u32,
    pages: Vec<Vec<u8>>,
}

impl Table {
    fn new() -> Self {
        // Initialize the pages vector with None values
        let pages: Vec<Vec<u8>> = Vec::new();
        // for _ in 0..TABLE_MAX_PAGES {
        //     pages.push(None);
        // }
        Table { num_rows: 0, pages }
    }
}

struct Row {
    id: i32,
    username: String,
    email: String,
}

fn main() {
    println!("Initialized QBA-DB version 0.0.1");

    let mut table = Table::new();

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
                    println!("Unrecognized command {}", user_input);
                    continue;
                }
            }
        }

        // prepare statement
        let mut cur_statement: Statement = Statement {
            statement_type: StatementType::Select,
            row_to_insert: Row {
                id: -1,
                username: "".to_string(),
                email: "".to_string(),
            },
        };

        match prepare_statement(&user_input, &mut cur_statement) {
            StatementPrepareResponse::Success => {
                execute_statement(cur_statement, &mut table);
            }
            StatementPrepareResponse::UnrecognizedCommand => {
                println!("Unrecognized statement {}", user_input);
                continue;
            }
            StatementPrepareResponse::SyntaxError => {
                println!("Syntax error in statement {}", user_input);
                continue;
            }
        }
    }
}

fn print_prompt() {
    print!("qba-db> ");
}

enum MetaCommandResponse {
    Success,
    UnrecognizedCommand,
}

fn perform_meta_command(command: &String) -> MetaCommandResponse {
    if command == ".exit" {
        exit(0);
    } else if command == ".ping" {
        println!("pong!");
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

        statement.row_to_insert.id = row_args[1].parse::<i32>().unwrap();
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
    for i in 0..table.num_rows {
        let row_slot = get_table_row(table, i).unwrap();

        let mut row_data = Row {
            id: 123,
            email: String::from("123"),
            username: String::from("!@3"),
        };
        deserialize_row(row_slot, &mut row_data);

        println!(
            "Row {}, id: {}, username: {}, email: {}",
            i, row_data.id, row_data.username, row_data.email
        );
    }

    Ok(())
}

fn execute_insert_statement(statement: Statement, table: &mut Table) -> Result<(), &'static str> {
    // insert from statement into table page
    let num_rows = table.num_rows;
    let row_slot = get_table_row(table, num_rows).unwrap();
    table.num_rows = num_rows + 1;

    serialize_row(&statement.row_to_insert, row_slot);

    Ok(())
}

fn get_table_row(table: &mut Table, row_num: u32) -> Result<*mut u8, &str> {
    let page_num: usize = (row_num / ROWS_PER_PAGE) as usize;
    // println!("total pages: {}", table.pages.len());

    if page_num >= table.pages.len() {
        let allocated = vec![0u8; PAGE_SIZE as usize];
        table.pages.push(allocated);
    }

    let cur_page = table.pages.get_mut(page_num);

    // println!("fetching from pages {}", page_num);

    match cur_page {
        Some(page) => unsafe {
            // println!(
            //     "Has page at address: {:?}. Adding {} rows",
            //     page.as_ptr(),
            //     row_num
            // );
            return Ok(page.as_mut_ptr().add(ROW_SIZE * row_num as usize));
        },
        None => return Err("Error fetching page from table"),
    }
}

fn serialize_row(source: &Row, destination: *mut u8) {
    unsafe { unsafe_serialize_row(source, destination) }
}

fn deserialize_row(source: *const u8, destination: &mut Row) {
    unsafe { unsafe_deserialize_row(source, destination) }
}

unsafe fn unsafe_serialize_row(source: &Row, destination: *mut u8) {
    // Serialize ID
    std::ptr::copy_nonoverlapping(
        &source.id as *const _ as *const u8,
        destination.offset(ID_OFFSET as isize),
        ID_SIZE,
    );

    // Serialize Username
    let username_bytes = source.username.as_bytes();
    let serialize_username_len = usize::min(USERNAME_SIZE, username_bytes.len());
    std::ptr::write_bytes(
        destination.offset(USERNAME_OFFSET as isize),
        0,
        USERNAME_SIZE,
    );
    std::ptr::copy_nonoverlapping(
        username_bytes.as_ptr(),
        destination.offset(USERNAME_OFFSET as isize),
        serialize_username_len,
    );

    // Serialize Email
    let email_bytes = source.email.as_bytes();
    let serialize_email_len = usize::min(EMAIL_SIZE, email_bytes.len());
    std::ptr::write_bytes(destination.offset(EMAIL_OFFSET as isize), 0, EMAIL_SIZE);
    std::ptr::copy_nonoverlapping(
        email_bytes.as_ptr(),
        destination.offset(EMAIL_OFFSET as isize),
        serialize_email_len,
    );
}

unsafe fn unsafe_deserialize_row(source: *const u8, destination: &mut Row) {
    // SAFER: Deserialize ID
    let id_slice = std::slice::from_raw_parts(source.offset(ID_OFFSET as isize), ID_SIZE);
    let id = i32::from_ne_bytes(id_slice.try_into().unwrap());

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

    // let source = source as *const u8;
    // println!(
    //     "step 1: deserializing id from {:?}",
    //     source.offset(ID_OFFSET as isize),
    // );
    // std::ptr::copy_nonoverlapping(
    //     source.offset(ID_OFFSET as isize),
    //     &mut destination.id as *mut _ as *mut u8,
    //     ID_SIZE,
    // );
    //
    // println!(
    //     "step 2: deserializing username from {:?}",
    //     source.offset(USERNAME_OFFSET as isize),
    // );
    // std::ptr::copy_nonoverlapping(
    //     source.offset(USERNAME_OFFSET as isize),
    //     destination.username.as_mut_ptr(),
    //     USERNAME_SIZE,
    // );
    // println!(
    //     "step 2: deserializing email from {:?}",
    //     source.offset(EMAIL_OFFSET as isize),
    // );
    // std::ptr::copy_nonoverlapping(
    //     source.offset(EMAIL_OFFSET as isize),
    //     destination.email.as_mut_ptr(),
    //     EMAIL_SIZE,
    // );
}
