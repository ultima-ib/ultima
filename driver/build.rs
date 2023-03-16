use std::path::Path;

use static_files::resource_dir;

fn main() -> std::io::Result<()> {
    let path = std::env::var("STATIC_FILES_DIR").unwrap_or_else(|_| {
        Path::new("..")
            .join("frontend")
            .join("dist")
            .into_os_string()
            .into_string()
            .unwrap()
    });

    resource_dir(dbg!(path)).build()
}
