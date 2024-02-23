use anyhow::Result;
use aws_config;
use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use aws_sdk_s3;
use aws_sdk_s3::types::Object;
use clap::Parser;
use derive_builder::Builder;
use std::collections::VecDeque;

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

    let mut iter = S3IterBuilder::default()
        .client(&client)
        .bucket(&bucket)
        .build()?;

    let mut count = 0;
    while let Some(object) = iter.next().await? {
        eprintln!("{}", object.key.unwrap_or_default());
        count += 1;
    }

    println!("Found {} objects", count);
    Ok(())
}

#[derive(Builder)]
pub struct S3Iter<'a> {
    bucket: &'a str,
    client: &'a aws_sdk_s3::Client,

    #[builder(private, setter(skip))]
    next_continuation_token: Option<String>,
    #[builder(private, setter(skip))]
    contents: VecDeque<Object>,
    #[builder(private, setter(skip))]
    is_truncated: Option<bool>,
}

impl S3Iter<'_> {
    pub async fn next(&mut self) -> Result<Option<Object>> {
        match (self.contents.pop_front(), self.is_truncated) {
            // Most common case: we have objects
            (Some(object), _) => Ok(Some(object)),

            // Next most common case, and also the true entry point of this function
            (None, None) | (None, Some(true)) => {
                // Note that in a real application, you'd need to deal with rate limits, errors, and retries here.
                let result = self
                    .client
                    .list_objects_v2()
                    .bucket(self.bucket)
                    .set_continuation_token(self.next_continuation_token.to_owned())
                    .send()
                    .await?;

                self.contents = result.contents.unwrap_or_default().into();
                self.next_continuation_token = result.next_continuation_token;
                self.is_truncated = result.is_truncated;

                Ok(self.contents.pop_front())
            }

            // Least common case: we have no objects and we're not expecting any more
            (None, Some(false)) => Ok(None),
        }
    }
}
