use openresponses_rust::{FunctionOutput, Input, Item, MessageContent};
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

        let mut cleaned = Vec::with_capacity(self.items.len());
        let mut pending_unanswered_calls: Vec<String> = Vec::new();

        for raw_item in &self.items {
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

            cleaned.push(strip_reasoning_summary(raw_item.clone()));
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
