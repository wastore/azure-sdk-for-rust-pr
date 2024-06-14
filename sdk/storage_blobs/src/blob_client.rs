use crate::{
    base_client::BaseClient,
    units::{Append, BlobKind, Block, Page, Unset},
};
use azure_core::{auth::TokenCredential, Context, Method, Pipeline, Request, Response, Url};
use std::{marker::PhantomData, sync::Arc};
pub struct BlobClient<T>
where
    T: BlobKind,
{
    account_name: String,
    credential: Arc<dyn TokenCredential>,
    container_name: String,
    blob_name: String,
    blob_type: PhantomData<T>,
    url: Url,
    pipeline: Pipeline,
}

// Even just this empty block will give us access to BaseClient's traits
impl BaseClient for BlobClient<Unset> {}

impl BlobClient<Unset> {
    pub fn new(
        account_name: String,
        credential: String,
        container_name: String,
        blob_name: String,
    ) -> Self {
        // Build BlobClient-specific URL
        let blob_url = BlobClient::build_blob_url(
            &BlobClient::build_url(&account_name, "blob"),
            &container_name,
            &blob_name,
        );

        // Get Credential Object
        let credential = BlobClient::get_credential();

        // Build our BlobClient
        Self {
            account_name: account_name,
            credential: credential.clone(),
            container_name: container_name,
            blob_name: blob_name,
            blob_type: Default::default(),
            url: Url::parse(&blob_url).expect("Something went wrong with URL parsing!"),
            pipeline: BlobClient::build_pipeline(credential),
        }
    }

    // This will handle appending container and blob name
    fn build_blob_url(base_url: &str, container_name: &str, blob_name: &str) -> String {
        base_url.to_owned() + container_name + "/" + blob_name
    }

    pub fn as_block_blob(self) -> BlobClient<Block> {
        BlobClient::<Block>::new(self)
    }

    pub async fn download_blob(&self) -> String {
        // Build the download request itself
        let mut request = Request::new(self.url.to_owned(), Method::Get); // This is technically cloning
        BlobClient::finalize_request(&mut request);

        // Send the request
        let response = self.pipeline.send(&(Context::new()), &mut request).await;
        println!("Response headers: {:?}", response);

        // Look at request body
        let response_body = response.unwrap().into_body().collect_string().await;
        println!("Response body: {:?}", response_body);

        // Return the body
        response_body.unwrap()
    }

    pub async fn get_blob_properties(&self) -> Response {
        // Build the get properties request itself
        let mut request = Request::new(self.url.to_owned(), Method::Head); // This is technically cloning
        BlobClient::finalize_request(&mut request);

        // Send the request
        let response = self.pipeline.send(&(Context::new()), &mut request).await;
        println!("Response headers: {:?}", response);

        // Return the response headers
        response.unwrap()
    }
}

impl BlobClient<Block> {
    pub fn new(client: BlobClient<Unset>) -> BlobClient<Block> {
        // Build our BlobClient
        Self {
            account_name: client.account_name,
            credential: client.credential.clone(),
            container_name: client.container_name,
            blob_name: client.blob_name,
            blob_type: Default::default(),
            url: client.url,
            pipeline: client.pipeline,
        }
    }

    pub fn upload_block_blob() {
        println!("uploaded block blob")
    }
}

// impl BlobClient<Page> {
//     pub fn new(client: BlobClient<Unset>) -> Self {
//         // Build our BlobClient
//         Self {
//             account_name: client.account_name,
//             credential: client.credential.clone(),
//             container_name: client.container_name,
//             blob_name: client.blob_name,
//             blob_type: Default::default(),
//             url: client.url,
//             pipeline: client.pipeline,
//         }
//     }

//     fn upload_page_blob() {
//         println!("uploaded page blob")
//     }
// }

// impl BlobClient<Append> {
//     pub fn new(client: BlobClient<Unset>) -> Self {
//         // Build our BlobClient
//         Self {
//             account_name: client.account_name,
//             credential: client.credential.clone(),
//             container_name: client.container_name,
//             blob_name: client.blob_name,
//             blob_type: Default::default(),
//             url: client.url,
//             pipeline: client.pipeline,
//         }
//     }

//     pub fn upload_append_blob() {
//         println!("uploaded append blob")
//     }
// }

#[cfg(test)]
mod tests {
    use azure_core::headers::HeaderName;

    use crate::{units::Unset, BlobClient};

    #[tokio::test]
    async fn test_download_blob() {
        // Create a Blob Client
        let my_blob_client = BlobClient::<Unset>::new(
            "vincenttranstock".to_string(),
            "throwaway".to_string(),
            "acontainer108f32e8".to_string(),
            "hello.txt".to_string(),
        );

        // Assert equality
        assert_eq!(my_blob_client.download_blob().await, "rustaceans")
    }

    #[tokio::test]
    async fn test_get_blob_properties() {
        // Create a Blob Client
        let my_blob_client = BlobClient::<Unset>::new(
            "vincenttranstock".to_string(),
            "throwaway".to_string(),
            "acontainer108f32e8".to_string(),
            "hello.txt".to_string(),
        );

        // Get response
        let ret = my_blob_client.get_blob_properties().await;
        let (status_code, headers, response_body) = ret.deconstruct();

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
    async fn test_blob_types() {
        // Create a Blob Client
        let my_blob_client = BlobClient::<Unset>::new(
            "vincenttranstock".to_string(),
            "throwaway".to_string(),
            "acontainer108f32e8".to_string(),
            "hello.txt".to_string(),
        );

        let block_blob_client = my_blob_client.as_block_blob();
        block_blob_client.upload_block_blob()
    }
}
