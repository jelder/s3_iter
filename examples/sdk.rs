use anyhow::Result;
use aws_config;
use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use aws_sdk_s3;
use clap::Parser;

#[derive(Parser)]
struct Args {
    #[clap(short, long)]
    bucket: String,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();
    let bucket = args.bucket;

    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;

    let client = aws_sdk_s3::Client::new(&config);
    let mut response = client
        .list_objects_v2()
        .bucket(bucket.to_owned())
        .into_paginator()
        .send();

    let mut count = 0;
    while let Some(result) = response.next().await {
        match result {
            Ok(output) => {
                for object in output.contents() {
                    println!("{}", object.key().unwrap_or_default());
                    count += 1;
                }
            }
            Err(err) => {
                eprintln!("{err:?}")
            }
        }
    }

    println!("Found {} objects", count);

    Ok(())
}
