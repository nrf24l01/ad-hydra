pub mod diagnostics;
mod favorites;
mod http;
mod repository;
mod retention;
mod search;
mod text;
mod visibility;
mod ws;
pub use http::TrafficApi;
pub use repository::{IngestResult, LiveCounters, LiveSockets, Page, Query, Traffic};
pub use search::{SearchCoverage, SearchCursor, SearchKind, SearchPage, SearchQuery, SearchScope};

/// Traffic routes. The page-scoped WS lives here beside the unchanged SSE route.
pub fn router(t: Traffic) -> axum::Router {
    let socket = axum::Router::new()
        .route(
            "/api/v1/traffic/events/socket",
            axum::routing::get(ws::socket),
        )
        .with_state(t.clone());
    http::router(t).merge(socket)
}
#[cfg(test)]
mod tests;

pub use repository::saved_context;
#[cfg(test)]
use repository::saved_fields;

#[cfg(test)]
mod retention_tests;
#[cfg(test)]
mod search_tests;
#[cfg(test)]
mod ws_regression_tests;
#[cfg(test)]
mod ws_tests;
