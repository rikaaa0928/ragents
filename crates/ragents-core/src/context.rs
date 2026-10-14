use openresponses_rust::{Input, Item, MessageContent};

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
        Input::Items(self.items.clone())
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
