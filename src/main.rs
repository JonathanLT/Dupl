use rusqlite::{Connection, Result};
use sha256::try_digest;
use std::path::{Path, PathBuf};
use clap::Parser;
use tqdm::tqdm;

extern crate glob;
use glob::glob;

#[derive(Parser, Debug)]
#[command(name = "Dupl")]
#[command(author = "JLO. <jonthan.lt@gmail.com>")]
#[command(version = "0.0.1")]
#[command(about = "Dupl save sha256 of all files following pattern", long_about = None)]
struct Args {
    /// Pattern for glob
    #[arg(short, long)]
    pattern: String,

    /// SQLite db file
    #[arg(long)]
    dbfile: Option<PathBuf>,

    /// Truncate SQLite db
    #[arg(short, long)]
    truncate: Option<bool>,
}

#[derive(Debug)]
struct File {
    id: Option<u64>,
    path: String,
    shasum: String,
    count: Option<u64>,
}

fn main() -> Result<()> {
    let pattern: &str;
    let truncate: bool;
    let mut path_db: PathBuf = PathBuf::from("./my_dupl.db3");

    let args: Args = Args::parse();
    pattern = &args.pattern;
    truncate = if !args.truncate.is_none() { args.truncate == Some(true) } else { true };
    path_db = if args.dbfile.is_some() { args.dbfile.unwrap() } else { path_db };

    println!("Pattern: {}", pattern);
    println!("Truncate: {:#?}", truncate);
    println!("Path DB: {:#?}", path_db);
    
    let conn: Connection = Connection::open(path_db)?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS file (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            path    TEXT NOT NULL,
            shasum  TEXT NOT NULL
        );",
        (), // empty list of parameters.
    )?;
    if truncate {
        conn.execute(
            "DELETE FROM file;",
            (), // empty list of parameters.
        )?;
    };

    for path in tqdm(glob(&pattern).unwrap().filter_map(Result::ok)).style(tqdm::Style::Block) {
        let input: &Path = Path::new(&path);
        if input.is_file() {
            let val: String = try_digest(input).unwrap();
            let f: File = File {
                id: Some(0),
                path: path.display().to_string(),
                shasum: val,
                count: Some(0),
            };
            let _ = &f.id; // To have no dead code
            let _ = &f.count; // To have no dead code
            conn.execute(
                "INSERT INTO file (path, shasum) VALUES (?1, ?2)",
                (&f.path, &f.shasum),
            )?;
        }
    }

    let mut stmt: rusqlite::Statement<'_> = conn.prepare("SELECT id, path, shasum, COUNT(*) AS \"count\" FROM file GROUP BY shasum HAVING COUNT(*) > 1 ORDER BY id;")?;
    let file_iter = stmt.query_map([], |row| {
        Ok(File {
            id: Some(row.get(0)?),
            path: row.get(1)?,
            shasum: row.get(2)?,
            count: row.get(3)?,
        })
    })?;

    for file in file_iter {
        println!("Found {:?}", file.unwrap());
    }

    Ok(())
}