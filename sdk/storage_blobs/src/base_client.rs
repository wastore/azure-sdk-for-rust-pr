use azure_core::{
    auth::TokenCredential, policies::BearerTokenCredentialPolicy, ClientOptions, Pipeline, Policy,
    Request,
};
use azure_identity::create_credential;
use std::sync::Arc;

pub(crate) trait BaseClient {
    fn build_pipeline(credential: Arc<dyn TokenCredential>) -> Pipeline {
        let oauth_token_policy =
            BearerTokenCredentialPolicy::new(credential, &["https://storage.azure.com/.default"]);
        let pipeline = Pipeline::new(
            option_env!("CARGO_PKG_NAME"),
            option_env!("CARGO_PKG_VERSION"),
            ClientOptions::default(),
            vec![Arc::new(oauth_token_policy) as Arc<dyn Policy>],
            Vec::new(),
        );
        pipeline
    }

    fn build_url(account_name: &str, service: &str) -> String {
        // Check Service
        if !(["blob", "queue", "file-share", "dfs"].contains(&service)) {
            println!("Not a valid service. Exiting.");
            std::process::exit(1);
        }
        "https://".to_owned() + account_name + "." + service + ".core.windows.net/"
    }

    fn finalize_request(request: &mut Request) {
        request.insert_header("x-ms-version", "2023-11-03")
    }
}
