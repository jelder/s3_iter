use anyhow::Result;
use aws_config;
use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use aws_sdk_s3;
use aws_sdk_s3::operation::list_objects_v2::builders::ListObjectsV2FluentBuilder;
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

    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;

    let client = aws_sdk_s3::Client::new(&config);

    let mut iter = S3ObjectIter::new(client.list_objects_v2().bucket(args.bucket));

    let mut count = 0;
    while let Some(object) = iter.next().await? {
        eprintln!("{}", object.key.unwrap());
        count += 1;
    }

    println!("Found {} objects", count);
    Ok(())
}

pub struct S3ObjectIter {
    list_objects_v2_builder: ListObjectsV2FluentBuilder,
    state: State,
    queue: Vec<Object>,
}

enum State {
    /// Initial state, we don't yet know if there are more objects to fetch
    NotYetKnown,
    /// We know there are more objects to fetch, and this is the token to use
    Partial { continuation_token: String },
    /// We know there are no more objects to fetch
    Complete,
}

impl S3ObjectIter {
    pub fn new(list_objects_v2_builder: ListObjectsV2FluentBuilder) -> S3ObjectIter {
        S3ObjectIter {
            list_objects_v2_builder,
            state: State::NotYetKnown,
            queue: vec![],
        }
    }

    async fn fetch(&mut self) -> Result<()> {
        // This is where, in a real app, you'd handle errors and retries
        let result = self
            .list_objects_v2_builder
            .clone()
            .set_continuation_token(if let State::Partial { continuation_token } = &self.state {
                Some(continuation_token.to_owned())
            } else {
                None
            })
            .send()
            .await?;

        if let Some(continuation_token) = result.next_continuation_token {
            self.state = State::Partial { continuation_token };
        } else {
            self.state = State::Complete;
        }

        // Ensure that we can efficiently pop and return objects in the same
        // order we received them by reversing the list. Alternatively, we could
        // chose a VecDeque for this field, but that has slightly more overhead.
        self.queue = result.contents.unwrap_or_default();
        self.queue.reverse();

        Ok(())
    }

    pub async fn next(&mut self) -> Result<Option<Object>> {
        match (self.queue.pop(), &self.state) {
            // Arm 1:
            // The most common case: we have objects. Nothing else is relevant.
            // The `_` means "anything here in this tuple"
            (Some(object), _) => Ok(Some(object)),

            // Arm 2:
            // The next most common cases, making next (or first) API call.
            // The `{ .. }` means "anything here in this struct"
            (None, State::Partial { .. } | State::NotYetKnown) => {
                self.fetch().await?;
                Ok(self.queue.pop())
            }

            // Arm 3:
            // Least common case, nothing in queue and we're not expecting more
            (None, State::Complete) => Ok(None),
        }
    }
}
