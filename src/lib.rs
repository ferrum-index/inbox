//! # inbox
//!
//! Private-message intake. The service does exactly one thing: accept a
//! message from an authenticated user and record an [`InboxMessageSent`] event
//! through the transactional outbox. It stores no messages and delivers
//! nothing; the notification service picks the event up and delivers it
//! (push / in-app / email) to the recipient named in the event's audience.
//!
//! ```text
//! POST {namespace}/messages ──► tx { outbox.push(InboxMessageSent) } ──► 202
//!                                      │
//!                         outbox worker publishes after commit
//!                                      ▼
//!                    event stream ──► notification service ──► recipient
//! ```
//!
//! Wiring it to the notification service is configuration only: add
//! [`InboxMessageSent::SUBJECT`] (`inbox.message.sent`) to that service's
//! `PUSH_SUBJECTS` and/or `IN_APP_SUBJECTS`.

use actix_web::{HttpResponse, Responder, web, web::ServiceConfig};
use ferrumec::event::{Event, EventError, EventType, Outbox};
use ferrumec::{Infra, Module as FerrumecModule};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;
use viewset::web::{Identity, SessionMiddleware};

pub type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;

/// Longest message accepted, in characters.
pub const MAX_BODY_CHARS: usize = 4000;

/// The event published for every accepted message.
///
/// The recipient is carried in the event *audience* (not only in the payload),
/// because that is what the notification service routes on. The sender is
/// carried in `metadata.user_id`.
#[derive(Debug, Serialize, Deserialize)]
pub struct InboxMessageSent {
    pub message_id: Uuid,
    pub from: Uuid,
    pub to: Uuid,
    pub body: String,
}

impl EventType for InboxMessageSent {
    const SUBJECT: &'static str = "inbox.message.sent";
}

/// Builds the event for a message: producer `inbox`, sender as `user_id`,
/// recipient as the sole audience member, `message_id` as correlation id.
fn build_event(from: Uuid, to: Uuid, body: String) -> Event<InboxMessageSent> {
    let message_id = Uuid::new_v4();
    Event::new(InboxMessageSent {
        message_id,
        from,
        to,
        body,
    })
    .with_producer("inbox")
    .with_user_id(from)
    .with_correlation_id(message_id)
    .with_audience(vec![to])
}

/// What the outbox needs: a pool to open the transaction on, and the outbox.
struct Inbox {
    pool: PgPool,
    outbox: Outbox,
}

impl Inbox {
    /// Records the event in the outbox. The transaction holds nothing but the
    /// outbox row; the event is published only once it commits.
    async fn accept(&self, event: &Event<InboxMessageSent>) -> core::result::Result<(), EventError> {
        let mut tx = self.pool.begin().await?;
        self.outbox.push(event, &mut tx).await?;
        tx.commit().await?;
        Ok(())
    }
}

/// The inbox module. Mounts `POST {namespace}/messages` behind the session
/// middleware (the session cookie is resolved against the shared cache written
/// by the auth service).
pub struct InboxModule {
    inbox: Arc<Inbox>,
    session: Arc<SessionMiddleware>,
}

/// The name this module is referred to by, matching the other services.
pub use InboxModule as Module;

impl FerrumecModule for InboxModule {
    /// Starts the outbox publisher (creating `outbox_events` if needed) and
    /// builds the session middleware.
    async fn new(infra: impl Infra) -> Result<Self> {
        let pool = infra.database();
        let outbox = Outbox::new(pool.clone(), infra.event_stream()).await?;
        let session = Arc::new(SessionMiddleware::new(infra.cache_factory()));
        Ok(Self {
            inbox: Arc::new(Inbox { pool, outbox }),
            session,
        })
    }

    fn configure(self: Arc<Self>, cfg: &mut ServiceConfig, namespace: &str) {
        cfg.service(
            web::scope(namespace)
                .wrap(self.session.clone())
                .app_data(web::Data::from(self.inbox.clone()))
                .route("/messages", web::post().to(send)),
        );
    }
}

#[derive(Deserialize)]
struct SendMessage {
    /// Recipient user id.
    to: Uuid,
    body: String,
}

#[derive(Serialize)]
struct Accepted {
    message_id: Uuid,
}

/// `POST /messages` — `{ "to": "<user uuid>", "body": "..." }`.
///
/// `202 Accepted` means the event is durably recorded and will be published;
/// it does not mean the recipient has received anything.
async fn send(
    session: web::ReqData<Identity>,
    inbox: web::Data<Inbox>,
    body: web::Json<SendMessage>,
) -> impl Responder {
    let from = match Uuid::parse_str(&session.sub.to_string()) {
        Ok(id) => id,
        Err(_) => return HttpResponse::Unauthorized().finish(),
    };
    let SendMessage { to, body } = body.into_inner();

    if body.trim().is_empty() {
        return HttpResponse::BadRequest().body("body must not be empty");
    }
    if body.chars().count() > MAX_BODY_CHARS {
        return HttpResponse::BadRequest()
            .body(format!("body must be at most {MAX_BODY_CHARS} characters"));
    }

    let event = build_event(from, to, body);
    let message_id = event.payload.message_id;
    match inbox.accept(&event).await {
        Ok(()) => HttpResponse::Accepted().json(Accepted { message_id }),
        Err(e) => {
            tracing::error!(error = %e, %from, %to, "inbox: failed to record message in outbox");
            HttpResponse::InternalServerError().finish()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrumec::event::Identifier;

    #[test]
    fn event_is_addressed_to_the_recipient_only() {
        let (from, to) = (Uuid::new_v4(), Uuid::new_v4());
        let ev = build_event(from, to, "hi".into());

        assert_eq!(ev.metadata.user_id, Some(from));
        assert_eq!(ev.metadata.producer.as_deref(), Some("inbox"));
        assert_eq!(ev.metadata.correlation_id, Some(ev.payload.message_id));
        assert!(matches!(ev.metadata.audience.as_slice(), [Identifier::Uuid(u)] if *u == to));
        assert_eq!((ev.payload.from, ev.payload.to), (from, to));
    }

    #[test]
    fn wire_format_has_metadata_audience() {
        // The notification service reads `metadata.audience` from the raw JSON.
        let ev = build_event(Uuid::new_v4(), Uuid::new_v4(), "hi".into());
        let json: serde_json::Value = serde_json::to_value(&ev).unwrap();
        assert!(json["metadata"]["audience"].is_array());
        assert_eq!(json["payload"]["body"], "hi");
    }
}
