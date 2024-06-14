use std::sync::Arc;

use azure_core::{auth::TokenCredential, Context, Method, Pipeline, Request, Response, Url};

use crate::base_client::BaseClient;

pub struct BlobContainerClient {
    account_name: String,
    credential: Arc<dyn TokenCredential>,
    container_name: String,
    url: Url,
    pipeline: Pipeline,
}

// Even just this empty block will give us access to BaseClient's traits
impl BaseClient for BlobContainerClient {}

impl BlobContainerClient {
    pub fn new(
        account_name: String,
        container_name: String,
        credential: Arc<dyn TokenCredential>,
    ) -> Self {
        // Build ContainerClient-specific URL
        let container_url = BlobContainerClient::build_container_url(
            &BlobContainerClient::build_url(&account_name, "blob"),
            &container_name,
        );

        // Build our BlobContainerClient
        Self {
            account_name: account_name,
            credential: Arc::clone(&credential),
            container_name: container_name,
            url: Url::parse(&container_url).expect("Something went wrong with URL parsing!"),
            pipeline: BlobContainerClient::build_pipeline(credential),
        }
    }

    // This will handle appending container name
    fn build_container_url(base_url: &str, container_name: &str) -> String {
        base_url.to_owned() + container_name + "/" + "?restype=container"
    }

    pub async fn get_container_properties(&self) -> Response {
        // Build the get properties request itself
        let mut request = Request::new(self.url.to_owned(), Method::Head); // This is technically cloning
        BlobContainerClient::finalize_request(&mut request);

        // Send the request
        let response = self.pipeline.send(&(Context::new()), &mut request).await;
        println!("Response headers: {:?}", response);

        // Return the response headers
        response.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use azure_core::headers::HeaderName;
    use azure_identity::DefaultAzureCredentialBuilder;

    #[tokio::test]
    async fn test_get_container_properties() {
        let credential = DefaultAzureCredentialBuilder::default()
            .build()
            .map(|cred| Arc::new(cred) as Arc<dyn TokenCredential>)
            .expect("Failed to build credential");

        // Create a Container Client
        let my_blob_container_client = BlobContainerClient::new(
            String::from("vincenttranstock"),
            String::from("acontainer108f32e8"),
            credential,
        );

        // Get response
        let ret = my_blob_container_client.get_container_properties().await;
        let (status_code, headers, response_body) = ret.deconstruct();

        // Assert equality
        assert_eq!(status_code, azure_core::StatusCode::Ok);
        assert_eq!(
            headers
                .get_str(&HeaderName::from_static("content-length"))
                .expect("Failed getting content-length header"),
            "0"
        )
    }
}
