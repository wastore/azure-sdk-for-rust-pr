use crate::{
    base_client::BaseClient,
    units::{Append, BlobKind, Block, Page, Unset},
    BlobClientOptions,
};
use azure_core::{
    auth::TokenCredential, Context, Method, Pipeline, Request, Response, Result, Url,
};
use bytes::Bytes;
use std::sync::Arc;

pub struct BlobClient<T: BlobKind> {
    account_name: String,
    credential: Arc<dyn TokenCredential>,
    container_name: String,
    blob_name: String,
    url: Url,
    pipeline: Pipeline,
    state: T,
}

// Even just this empty block will give us access to BaseClient's traits
impl<T: BlobKind> BaseClient for BlobClient<T> {}

impl BlobClient<Unset> {
    pub fn new(
        account_name: String,
        container_name: String,
        blob_name: String,
        credential: Arc<dyn TokenCredential>,
        options: Option<BlobClientOptions>,
    ) -> Self {
        // Build BlobClient-specific URL
        let blob_url = BlobClient::<Unset>::build_blob_url(
            &BlobClient::<Unset>::build_url(&account_name, "blob"),
            &container_name,
            &blob_name,
        );

        let options = options.unwrap_or_default();

        // Build our BlobClient
        Self {
            account_name: account_name,
            credential: Arc::clone(&credential),
            container_name: container_name,
            blob_name: blob_name,
            url: Url::parse(&blob_url).expect("Something went wrong with URL parsing!"),
            pipeline: BlobClient::<Unset>::build_pipeline(credential, options.client_options),
            state: Unset,
        }
    }

    // We can make this alot better by adding the Default implements
    fn as_block_blob(self) -> BlobClient<Block> {
        BlobClient {
            account_name: self.account_name,
            credential: self.credential,
            container_name: self.container_name,
            blob_name: self.blob_name,
            url: self.url,
            pipeline: self.pipeline,
            state: Block,
        }
    }

    fn as_page_blob(self) -> BlobClient<Page> {
        BlobClient {
            account_name: self.account_name,
            credential: self.credential,
            container_name: self.container_name,
            blob_name: self.blob_name,
            url: self.url,
            pipeline: self.pipeline,
            state: Page,
        }
    }

    fn as_append_blob(self) -> BlobClient<Append> {
        BlobClient {
            account_name: self.account_name,
            credential: self.credential,
            container_name: self.container_name,
            blob_name: self.blob_name,
            url: self.url,
            pipeline: self.pipeline,
            state: Append,
        }
    }

    // This will handle appending container and blob name
    fn build_blob_url(base_url: &str, container_name: &str, blob_name: &str) -> String {
        base_url.to_owned() + container_name + "/" + blob_name
    }
}

impl<T: BlobKind> BlobClient<T> {
    pub async fn download_blob(&self) -> Result<Bytes> {
        // Build the download request itself
        let mut request = Request::new(self.url.to_owned(), Method::Get); // This is technically cloning
        BlobClient::<T>::finalize_request(&mut request);

        // Send the request
        let response = self.pipeline.send(&(Context::new()), &mut request).await?;
        println!("Response headers: {:?}", response);

        // Look at request body
        let response_body = response.into_body().collect().await?;
        println!("Response body: {:?}", response_body);

        // Return the body
        Ok(response_body)
    }

    pub async fn get_blob_properties(&self) -> Result<Response> {
        // Build the get properties request itself
        let mut request = Request::new(self.url.to_owned(), Method::Head); // This is technically cloning
        BlobClient::<T>::finalize_request(&mut request);

        // Send the request
        let response = self.pipeline.send(&(Context::new()), &mut request).await?;
        println!("Response headers: {:?}", response);

        // Return the entire response for now
        Ok(response)
    }
}

impl BlobClient<Block> {
    fn upload_block_blob(&self) {
        println!("block")
    }
}

// Probably need to append ?comp=appendblock
impl BlobClient<Append> {
    fn upload_append_blob(&self) {
        println!("append")
    }
}

// Probably need to append ?comp=page
impl BlobClient<Page> {
    fn upload_page_blob(&self) {
        println!("page")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use azure_core::headers::HeaderName;
    use azure_identity::DefaultAzureCredentialBuilder;

    #[tokio::test]
    async fn test_download_blob() {
        let credential = DefaultAzureCredentialBuilder::default()
            .build()
            .map(|cred| Arc::new(cred) as Arc<dyn TokenCredential>)
            .expect("Failed to build credential");

        // Create a Blob Client
        let my_blob_client = BlobClient::new(
            String::from("vincenttranstock"),
            String::from("acontainer108f32e8"),
            String::from("hello.txt"),
            credential,
            Some(BlobClientOptions::default()),
        );
        let result = my_blob_client
            .download_blob()
            .await
            .expect("Request failed!");

        // Assert equality
        assert_eq!(result, b"rustaceans"[..])
    }

    #[tokio::test]
    async fn test_get_blob_properties() {
        let credential = DefaultAzureCredentialBuilder::default()
            .build()
            .map(|cred| Arc::new(cred) as Arc<dyn TokenCredential>)
            .expect("Failed to build credential");

        // Create a Blob Client
        let my_blob_client = BlobClient::new(
            String::from("vincenttranstock"),
            String::from("acontainer108f32e8"),
            String::from("hello.txt"),
            credential,
            None,
        );

        // Get response
        let ret = my_blob_client
            .get_blob_properties()
            .await
            .expect("Request failed!");
        let (status_code, headers, _response_body) = ret.deconstruct();

        // Assert equality
        assert_eq!(status_code, azure_core::StatusCode::Ok);
        assert_eq!(
            headers
                .get_str(&HeaderName::from_static("content-length"))
                .expect("Failed getting content-length header"),
            "10"
        )
    }

    #[tokio::test]
    async fn test_block_blob() {
        let credential = DefaultAzureCredentialBuilder::default()
            .build()
            .map(|cred| Arc::new(cred) as Arc<dyn TokenCredential>)
            .expect("Failed to build credential");

        // Create a Blob Client
        let my_blob_client = BlobClient::new(
            String::from("vincenttranstock"),
            String::from("acontainer108f32e8"),
            String::from("hello.txt"),
            credential,
            Some(BlobClientOptions::default()),
        );

        let block_blob = my_blob_client.as_block_blob();
        block_blob.upload_block_blob()
    }

    #[tokio::test]
    async fn test_append_blob() {
        let credential = DefaultAzureCredentialBuilder::default()
            .build()
            .map(|cred| Arc::new(cred) as Arc<dyn TokenCredential>)
            .expect("Failed to build credential");

        // Create a Blob Client
        let my_blob_client = BlobClient::new(
            String::from("vincenttranstock"),
            String::from("acontainer108f32e8"),
            String::from("hello.txt"),
            credential,
            Some(BlobClientOptions::default()),
        );

        let append_blob = my_blob_client.as_append_blob();
        append_blob.upload_append_blob()
    }

    #[tokio::test]
    async fn test_page_blob() {
        let credential = DefaultAzureCredentialBuilder::default()
            .build()
            .map(|cred| Arc::new(cred) as Arc<dyn TokenCredential>)
            .expect("Failed to build credential");

        // Create a Blob Client
        let my_blob_client = BlobClient::new(
            String::from("vincenttranstock"),
            String::from("acontainer108f32e8"),
            String::from("hello.txt"),
            credential,
            Some(BlobClientOptions::default()),
        );

        let page_blob = my_blob_client.as_page_blob();
        page_blob.upload_page_blob()
    }
}
