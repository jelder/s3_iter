use anyhow::Result;
use aws_config;
use aws_config::BehaviorVersion;
use aws_sdk_s3;
use clap::Parser;

#[derive(Parser)]
struct Args {
    #[clap(short, long)]
    bucket: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let bucket = args.bucket;

    let client =
        aws_sdk_s3::Client::new(&aws_config::defaults(BehaviorVersion::latest()).load().await);

    let mut response = client
        .list_objects_v2()
        .bucket(bucket)
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
