# inbox

Accepts private messages and hands them to the outbox. Delivery is the
notification service's job.

## API

`POST /inbox/messages` (session cookie required)

```json
{ "to": "3f2b…-recipient-user-uuid", "body": "hello" }
```

* `202` `{ "message_id": "…" }` — the event is recorded in the outbox in a
  transaction and will be published (at-least-once). It does not mean the
  recipient has received it.
* `400` empty / whitespace-only body, or longer than 4000 characters.
* `401` no session, or the session user id is not a UUID.

## Event

Subject `inbox.message.sent`, envelope `{ metadata, payload }`:

* `metadata.audience` = `[recipient uuid]`, `metadata.user_id` = sender,
  `metadata.producer` = `inbox`, `metadata.correlation_id` = `message_id`
* `payload` = `{ message_id, from, to, body }`

## Hooking up the notification service

Config only, on the notification service:

```
PUSH_SUBJECTS=...,inbox.message.sent      # live, if the recipient has a websocket open
IN_APP_SUBJECTS=...,inbox.message.sent    # stored; read with GET /in-app
```

(`IN_APP_SUBJECTS` defaults to `PUSH_SUBJECTS` when unset.) For email, add it
to `EMAIL_SUBJECTS` and provide a `templates/inbox.message.sent.html`; only
users who confirmed an email address for that subject receive it.

## Environment

`DATABASE_URL`, `REDIS_URL`, `NATS_URL` (ferrumec `RemoteInfra`), `BIND_ADDR`
(default `0.0.0.0:8080`).
