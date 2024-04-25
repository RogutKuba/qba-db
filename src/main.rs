use qba_db::db::Db;

fn main() {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();

    let mut db = Db::new(String::from("test.db"));
    db.run_db();

    db.close_db().unwrap();
}
