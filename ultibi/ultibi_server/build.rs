use static_files::resource_dir;

fn main() -> std::io::Result<()> {
    let path =
        std::env::var("STATIC_FILES_DIR").unwrap_or_else(|_| "../../frontend/dist".to_string());
        let paths = std::fs::read_dir("./").unwrap();

        dbg!("this level");

        for path in paths {
            dbg!(format!("{}", path.unwrap().path().display()));
        };

        dbg!("one level up");

        let paths = std::fs::read_dir("../").unwrap();

        for path in paths {
            dbg!(format!("{}", path.unwrap().path().display()));
        };

        dbg!("two levels up");

        let paths = std::fs::read_dir("../../").unwrap();

        for path in paths {
            dbg!(format!("{}", path.unwrap().path().display()));
        };

        dbg!("frontend");

        let paths = std::fs::read_dir("../../frontend/").unwrap();

        for path in paths {
            dbg!(format!("{}", path.unwrap().path().display()));
        };

    resource_dir(path).build()
}
