extern crate diesel;

use std::env;
use std::time::Duration;

use diesel::{ExpressionMethods, insert_into, QueryDsl, r2d2, RunQueryDsl};
use diesel::{pg::PgConnection};
use diesel::r2d2::ConnectionManager;
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use dotenvy::dotenv;

use kaspa_db_filler_ng::database::schema::blocks::dsl::blocks;
use kaspa_db_filler_ng::database::schema::blocks::hash;

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!();

fn main() {
    dotenv().ok();

    let url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let manager = ConnectionManager::<PgConnection>::new(&url);
    let pool = r2d2::Pool::builder()
        .test_on_check_out(true)
        .connection_timeout(Duration::from_secs(10))
        .build(manager)
        .expect("Could not build connection pool");
    let con = &mut pool.get().unwrap();
    println!("Connection to the database established!");

    if env::var("DEVELOPMENT").unwrap().eq_ignore_ascii_case("true") {
        println!("Applying pending migrations");
        con.run_pending_migrations(MIGRATIONS).unwrap();
    }

    println!("Inserting test data");
    for i in 0..9 {
        insert_into(blocks)
            .values(hash.eq(hex::decode(format!("aba{}", i)).expect("")))
            .on_conflict_do_nothing()
            .execute(con)
            .expect("Failed to insert test data");
    }

    println!("Fetching test data");
    let results = blocks
        .select(hash)
        .load::<Vec<u8>>(con)
        .expect("Failed to retrieve test data");

    println!("Displaying {} blocks:", results.len());
    for r in results {
        println!("hash: {}", hex::encode(r))
    }
}
