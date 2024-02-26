use anyhow::Result;
use aws_config;
use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use aws_sdk_s3;
use aws_sdk_s3::types::Object;
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

    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;

    let client = aws_sdk_s3::Client::new(&config);

    let mut iter = S3Iter::new(&client, &bucket);

    let mut count = 0;
    while let Some(object) = iter.next().await? {
        eprintln!("{}", object.key.unwrap_or_default());
        count += 1;
    }

    println!("Found {} objects", count);
    Ok(())
}

pub struct S3Iter<'a> {
    bucket: &'a str,
    client: &'a aws_sdk_s3::Client,

    next_continuation_token: Option<String>,
    contents: Vec<Object>,
    truncated: Truncation,
}

#[derive(Clone, Copy)]
enum Truncation {
    NotYetKnown,
    Truncated,
    NotTruncated,
}

impl S3Iter<'_> {
    pub fn new<'a>(client: &'a aws_sdk_s3::Client, bucket: &'a str) -> S3Iter<'a> {
        S3Iter {
            bucket,
            client,
            next_continuation_token: None,
            contents: Vec::new(),
            truncated: Truncation::NotYetKnown,
        }
    }

    async fn fetch(&mut self) -> Result<()> {
        let result = self
            .client
            .list_objects_v2()
            .bucket(self.bucket)
            .set_continuation_token(self.next_continuation_token.to_owned())
            .send()
            .await?;

        self.next_continuation_token = result.next_continuation_token;
        self.set_truncated(result.is_truncated.unwrap_or_default());

        self.contents = result.contents.unwrap_or_default();
        // Ensure that we can efficiently emit objects in the same order we received them.
        self.contents.reverse();

        Ok(())
    }

    fn set_truncated(&mut self, is_truncated: bool) {
        self.truncated = if is_truncated {
            Truncation::Truncated
        } else {
            Truncation::NotTruncated
        }
    }

    pub async fn next(&mut self) -> Result<Option<Object>> {
        match (self.contents.pop(), self.truncated) {
            // Branch 1:
            // The most common case: we have objects
            (Some(object), _) => Ok(Some(object)),

            // Branch 2:
            // The next most common cases, making next (or first) API call.
            (None, Truncation::Truncated | Truncation::NotYetKnown) => {
                self.fetch().await?;
                Ok(self.contents.pop())
            }

            // Branch 3:
            // Least common case, nothing in queue and we're not expecting more
            (None, Truncation::NotTruncated) => Ok(None),
        }
    }
}
