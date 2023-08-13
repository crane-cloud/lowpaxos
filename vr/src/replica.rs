use vr::node::VsrReplica;
use clap::Arg;
use vr::utility::parse_configuration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let matches = clap::App::new("VR Replica")
        .version("0.1.0")
        .arg(Arg::with_name("config")
            .short('c')
            .long("config")
            .value_name("FILE")
            .help("Sets a custom config file")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("index")
            .short('i')
            .long("index")
            .value_name("INDEX")
            .help("Sets the replica index id")
            .takes_value(true)
            .required(true))
        .get_matches();

    let config_file = matches.value_of("config").unwrap();
    let index_id = matches.value_of("index").unwrap().parse::<usize>().expect("Invalid index");

    let config = parse_configuration(config_file);

    let vsr = VsrReplica::new(config, index_id, true, 0);
    vsr.await.start_vr_replica().await;

    Ok(())
}