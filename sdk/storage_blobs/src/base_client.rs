use azure_core::{
    auth::TokenCredential, policies::BearerTokenCredentialPolicy, ClientOptions, Pipeline, Policy,
    Request,
};
use std::sync::Arc;

pub(crate) trait BaseClient {
    fn build_pipeline(credential: Arc<dyn TokenCredential>, options: ClientOptions) -> Pipeline {
        let oauth_token_policy =
            BearerTokenCredentialPolicy::new(credential, &["https://storage.azure.com/.default"]);
        Pipeline::new(
            option_env!("CARGO_PKG_NAME"),
            option_env!("CARGO_PKG_VERSION"),
            options,
            vec![Arc::new(oauth_token_policy) as Arc<dyn Policy>],
            Vec::new(),
        )
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
