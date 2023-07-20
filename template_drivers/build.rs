use static_files::resource_dir;

fn main() -> std::io::Result<()> {
    dbg!("building");
    let path = std::env::var("STATIC_FILES_DIR").unwrap_or_else(|_| "../frontend/dist".to_string());
    resource_dir(dbg!(path)).build()
}
