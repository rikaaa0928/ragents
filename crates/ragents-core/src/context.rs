use openresponses_rust::{FunctionOutput, Input, Item, MessageContent, MessageRole};
use std::collections::HashSet;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ContextProjection {
    items: Vec<Item>,
}

impl ContextProjection {
    pub fn new(items: Vec<Item>) -> Self {
        Self { items }
    }

    pub fn items(&self) -> &[Item] {
        &self.items
    }

    pub fn into_items(self) -> Vec<Item> {
        self.items
    }

    pub fn to_openresponses_input(&self) -> Input {
        let answered_call_ids: HashSet<&str> = self
            .items
            .iter()
            .filter_map(|item| match item {
                Item::FunctionCallOutput { call_id, .. } => Some(call_id.as_str()),
                _ => None,
            })
            .collect();

        let last_user_msg_idx = self
            .items
            .iter()
            .rposition(|item| matches!(item, Item::Message { role: MessageRole::User, .. }))
            .unwrap_or(0);

        let mut cleaned = Vec::with_capacity(self.items.len());
        let mut pending_unanswered_calls: Vec<String> = Vec::new();
        let mut seen_output_call_ids: HashSet<&str> = HashSet::new();

        for (idx, raw_item) in self.items.iter().enumerate() {
            if matches!(raw_item, Item::Message { .. }) && !pending_unanswered_calls.is_empty() {
                for call_id in pending_unanswered_calls.drain(..) {
                    cleaned.push(Item::FunctionCallOutput {
                        id: None,
                        status: None,
                        call_id,
                        output: FunctionOutput::Text(
                            "[Tool execution was interrupted or cancelled before completion.]"
                                .to_string(),
                        ),
                    });
                }
            }

            if let Item::FunctionCall { call_id, .. } = raw_item {
                if !answered_call_ids.contains(call_id.as_str()) {
                    pending_unanswered_calls.push(call_id.clone());
                }
            }

            if let Item::FunctionCallOutput { call_id, .. } = raw_item {
                if !seen_output_call_ids.insert(call_id.as_str()) {
                    // Skip duplicate FunctionCallOutput for the same call_id
                    continue;
                }
            }

            let sanitized = strip_reasoning_summary(raw_item.clone());
            let sanitized = if idx < last_user_msg_idx {
                strip_historical_tool_image_data(sanitized)
            } else {
                sanitized
            };
            cleaned.push(sanitized);
        }

        for call_id in pending_unanswered_calls {
            cleaned.push(Item::FunctionCallOutput {
                id: None,
                status: None,
                call_id,
                output: FunctionOutput::Text(
                    "[Tool execution was interrupted or cancelled before completion.]".to_string(),
                ),
            });
        }

        Input::Items(cleaned)
    }
}

fn strip_historical_tool_image_data(item: Item) -> Item {
    match item {
        Item::FunctionCallOutput {
            id,
            status,
            call_id,
            output: FunctionOutput::Content(parts),
        } => {
            let new_parts: Vec<MessageContent> = parts
                .into_iter()
                .map(|part| match part {
                    MessageContent::InputImage {
                        image_url: Some(ref url),
                        ..
                    } if url.starts_with("data:") => MessageContent::InputText {
                        text: "[Historical image data omitted in subsequent turns to save bandwidth; call view_image again if you need to re-inspect this image.]".to_string(),
                    },
                    other => other,
                })
                .collect();
            Item::FunctionCallOutput {
                id,
                status,
                call_id,
                output: FunctionOutput::Content(new_parts),
            }
        }
        other => other,
    }
}

pub fn strip_reasoning_summary(item: Item) -> Item {
    match item {
        Item::Reasoning {
            id,
            status,
            summary,
            encrypted_content,
            ..
        } => Item::Reasoning {
            id,
            status,
            content: None,
            summary,
            encrypted_content,
        },
        other => other,
    }
}

pub fn extract_item_text(item: &Item) -> String {
    let mut out = String::new();
    match item {
        Item::Message { content, .. } => {
            for part in content {
                match part {
                    MessageContent::OutputText { text, .. }
                    | MessageContent::PlainText { text }
                    | MessageContent::InputText { text } => {
                        out.push_str(text);
                    }
                    MessageContent::Refusal { refusal } => {
                        out.push_str(refusal);
                    }
                    _ => {}
                }
            }
        }
        Item::Reasoning {
            summary, content, ..
        } => {
            for part in summary {
                match part {
                    MessageContent::SummaryText { text }
                    | MessageContent::OutputText { text, .. }
                    | MessageContent::PlainText { text } => {
                        out.push_str(text);
                    }
                    _ => {}
                }
            }
            if out.is_empty() {
                if let Some(content_parts) = content {
                    for part in content_parts {
                        match part {
                            MessageContent::SummaryText { text }
                            | MessageContent::OutputText { text, .. }
                            | MessageContent::PlainText { text } => {
                                out.push_str(text);
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        _ => {}
    }
    out
}
