fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = "../../proto";

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                format!("{proto_root}/market_data.proto"),
                format!("{proto_root}/execution.proto"),
                format!("{proto_root}/risk.proto"),
                format!("{proto_root}/strategy.proto"),
            ],
            &[proto_root],
        )?;

    Ok(())
}
