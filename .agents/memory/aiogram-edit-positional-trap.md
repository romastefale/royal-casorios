---
name: aiogram edit_message_* positional arg trap
description: In aiogram 3.x the 2nd positional arg of edit_message_* is business_connection_id, not chat_id — always pass chat_id/message_id as keywords.
---

# aiogram `edit_message_*` positional-arg trap

In aiogram 3.x (Bot API 7+), `bot.edit_message_text` / `edit_message_caption` /
`edit_message_reply_markup` / `edit_message_media` / `edit_message_live_location`
take **`business_connection_id` as the 2nd positional parameter**, BEFORE
`chat_id` (because for inline-message edits `chat_id` is optional). So a call like
`bot.edit_message_text(text, chat_id, message_id)` silently feeds `chat_id` into
`business_connection_id` → at runtime raises a pydantic
`ValidationError: business_connection_id Input should be a valid string
[input_value=<the chat_id int>]`.

**Rule:** ALWAYS pass `chat_id=` and `message_id=` as keyword args on every
`edit_message_*` call. Never rely on positional order after the text/caption.

**Why:** this shipped to production and broke the `/rquiz` lobby edits (the int
chat_id of the test group landed in `business_connection_id`). `send_*` methods
are NOT affected — there `chat_id` is still the 1st positional.

**How to apply:** when adding/reviewing any `edit_message_*` call, grep for
positional usage; the safe form is `bot.edit_message_text(text, chat_id=...,
message_id=..., ...)`.
