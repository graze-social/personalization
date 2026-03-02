//! Service layer for shared application components.

pub mod interactions;
pub mod special_posts;
mod uri_interner;

pub use interactions::{InteractionsClient, InteractionsConfig};
pub use special_posts::{
    SpecialPost, SpecialPostsClient, SpecialPostsResponse, SpecialPostsSource, SponsoredPost,
};
pub use uri_interner::UriInterner;
