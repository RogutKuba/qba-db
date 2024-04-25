use qba_db::db::Db;

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use super::*;

    fn init() {
        // delete test.db on each run
        let file_exists = Path::exists(Path::new("test.db"));
        if file_exists {
            fs::remove_file("test.db").unwrap();
        }

        std::env::set_var("RUST_LOG", "info");
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn basic_insert_test() {
        init();
        let mut db = Db::new(String::from("test.db"));

        let insert_command = String::from("insert 1 test_user test_email");
        db.run_db_test(insert_command);

        let insert_command = String::from("insert 2 test_user_2 test_email_2");
        db.run_db_test(insert_command);

        let select_command = String::from("select");
        db.run_db_test(select_command);
    }

    #[test]
    fn page_full_test() {
        init();
        let mut db = Db::new(String::from("test.db"));

        for _ in 0..2 {
            let insert_command = String::from("insert 1 test_user test_email");
            db.run_db_test(insert_command);
        }

        let select_command = String::from("select");
        db.run_db_test(select_command);

        // let page_num = {
        //     let mut count = 0;
        //     for p in db.table.pager.pages {
        //         match p {
        //             Some(_) => count = count + 1,
        //             None => {}
        //         }
        //     }

        //     count
        // };

        // assert_eq!(page_num, 2, "Table has wrong number of pages!");
    }

    #[test]
    fn insert_max_string_test() {
        init();
        let mut db = Db::new(String::from("test.db"));

        let insert_command = String::from(
            "insert 1 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabbcc test_email",
        );
        db.run_db_test(insert_command);

        let select_command = String::from("select");
        db.run_db_test(select_command);
    }
}
