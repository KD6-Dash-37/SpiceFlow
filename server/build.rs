use std::fs;
use std::process::Command;

fn main() {
    // dotenv().ok();
    // env_logger::init();
    println!("Starting build script...");

    // Define the paths for the templates and generated directories
    let templates_dir = "../flat-buffer/templates";
    let rust_gen_dir = "../server/src/gen_templates";

    // Ensure the directories exist
    fs::create_dir_all(rust_gen_dir)
        .expect("Failed to create the Rust flat-buffer generation directory");

    // Iterate over all .fbs files in the templates directory
    let schema_files = fs::read_dir(templates_dir).expect("Failed to read templates directory");

    for schema in schema_files {
        let schema = schema.expect("Failed to read flat-buffer schema file");
        let path = schema.path();

        if path.extension().and_then(|ext| ext.to_str()) == Some("fbs") {
            let schema_path = path
                .to_str()
                .expect("Failed to to convert schema path to string");

            let rust_status = Command::new("flatc")
                .args([
                    "--rust",
                    "-o",
                    rust_gen_dir,
                    schema_path,
                    "--filename-suffix",
                    "",
                    "--gen-all",
                    "--gen-object-api",
                ])
                .status()
                .expect("Failed to execute flatc for Rust");

            assert!(rust_status.success(), "flatc failed for Rust templates");
        }
    }
}
