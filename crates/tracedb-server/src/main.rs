#![forbid(unsafe_code)]

use std::env;
use std::path::PathBuf;

fn main() {
    if let Err(error) = run() {
        eprintln!("tracedb-server: {error}");
        std::process::exit(1);
    }
}

fn run() -> std::io::Result<()> {
    let service_mode = env::var("TRACEDB_SERVICE_MODE").unwrap_or_else(|_| "engine".to_string());
    if service_mode == "gateway" {
        return tracedb_gateway::serve(tracedb_gateway::GatewayServerConfig::from_env());
    }
    let data_dir = env::var_os("TRACEDB_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(".tracedb"));
    let bind = bind_addr_from_env();
    tracedb_server::serve(data_dir, &bind)
}

fn bind_addr_from_env() -> String {
    env::var("TRACEDB_BIND").unwrap_or_else(|_| {
        env::var("PORT")
            .map(|port| format!("[::]:{port}"))
            .unwrap_or_else(|_| "[::]:8080".to_string())
    })
}
