use std::process::{Command, Output};

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_dt-main"))
        .args(args)
        .env_remove("RUST_BACKTRACE")
        .env_remove("RUST_LIB_BACKTRACE")
        .output()
        .unwrap()
}

#[test]
fn missing_config_is_argument_error_before_logger_initialization() {
    let output = run(&[]);
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains(
            "--config-path, --config, or positional CONFIG is required when --config-source=local"
        ),
        "{stderr}"
    );
    assert!(!stderr.contains("panicked"), "{stderr}");
    assert!(!stderr.contains("__APE_DTS_ERROR_CONTEXT__"), "{stderr}");
    assert!(!stderr.contains("DIAGNOSTIC"), "{stderr}");
}
