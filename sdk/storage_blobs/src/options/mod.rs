use azure_core::ClientOptions;

#[derive(Clone, Debug)]
pub struct BlobClientOptions {
    pub(crate) api_version: Option<String>,
    pub(crate) client_options: ClientOptions,
}

impl BlobClientOptions {
    pub fn builder() -> builders::BlobClientOptionsBuilder {
        builders::BlobClientOptionsBuilder::new()
    }
}

impl Default for BlobClientOptions {
    fn default() -> Self {
        Self {
            api_version: Some(String::from("2023-11-03")),
            client_options: ClientOptions::default(),
        }
    }
}

pub mod builders {
    use super::*;

    pub struct BlobClientOptionsBuilder {
        options: BlobClientOptions,
    }

    impl BlobClientOptionsBuilder {
        pub(super) fn new() -> Self {
            Self {
                options: BlobClientOptions::default(),
            }
        }

        pub fn with_api_version(mut self, api_version: impl Into<String>) -> Self {
            self.options.api_version = Some(api_version.into());
            self
        }

        // TODO: This probably isn't correct
        pub fn with_client_options(mut self, client_options: ClientOptions) -> Self {
            self.options.client_options = client_options;
            self
        }

        pub fn build(&self) -> BlobClientOptions {
            self.options.clone()
        }
    }
}

mod tests {
    use super::*;
    use azure_core::{FixedRetryOptions, RetryOptions};

    #[test]
    fn test_blob_client_options_builder() {
        let client_options =
            ClientOptions::default().retry(RetryOptions::fixed(FixedRetryOptions::default()));

        let version = "12345";
        let options = BlobClientOptions::builder()
            .with_api_version(version)
            .with_client_options(client_options)
            .build();

        assert_eq!(options.api_version, Some(version.to_string()));
        // Not sure how to assert client_options because they are all private
    }
}
