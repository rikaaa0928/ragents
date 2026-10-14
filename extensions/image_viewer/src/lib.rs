use base64::Engine;
use serde::{Deserialize, Serialize};
use std::cell::Cell;
use std::fs;
use std::path::Path;

wit_bindgen::generate!({
    path: "../../wit",
    world: "plugin",
});

const AGENT_PREPARE_HOOK: &str = "agent.prepare";
const TOOLS_CALL_HOOK: &str = "tools.call";
const DEFAULT_MAX_FILE_SIZE_BYTES: u64 = 20 * 1024 * 1024; // 20 MB

thread_local! {
    static MAX_FILE_SIZE_BYTES: Cell<u64> = const { Cell::new(DEFAULT_MAX_FILE_SIZE_BYTES) };
}

struct ImageViewerExtension;

#[derive(Serialize)]
struct Metadata<'a> {
    id: &'a str,
    version: &'a str,
    subscriptions: Vec<Subscription<'a>>,
}

#[derive(Serialize)]
struct Subscription<'a> {
    hook: &'a str,
    kind: &'a str,
    priority: i32,
    failure: &'a str,
}

#[derive(Deserialize)]
struct HookRequest {
    hook: String,
    payload: serde_json::Value,
}

#[derive(Deserialize)]
struct ToolCallRequest {
    name: String,
    arguments: serde_json::Value,
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum MessageContentPart {
    #[serde(rename = "input_text")]
    InputText { text: String },
    #[serde(rename = "input_image")]
    InputImage { image_url: Option<String> },
}

#[derive(Serialize)]
#[serde(untagged)]
enum ToolOutputPayload {
    Text(String),
    Parts(Vec<MessageContentPart>),
}

#[derive(Serialize)]
struct ToolResult {
    success: bool,
    output: ToolOutputPayload,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Default, Deserialize)]
struct ImageViewerConfig {
    max_file_size_bytes: Option<u64>,
}

#[derive(Deserialize)]
struct ViewImageArgs {
    path: String,
    #[serde(default = "default_include_base64")]
    include_base64: bool,
}

fn default_include_base64() -> bool {
    true
}

impl exports::ragent::extension::lifecycle::Guest for ImageViewerExtension {
    fn metadata() -> String {
        serde_json::to_string(&Metadata {
            id: "image_viewer",
            version: env!("CARGO_PKG_VERSION"),
            subscriptions: vec![
                Subscription {
                    hook: AGENT_PREPARE_HOOK,
                    kind: "transform",
                    priority: 100,
                    failure: "abort",
                },
                Subscription {
                    hook: TOOLS_CALL_HOOK,
                    kind: "action",
                    priority: 100,
                    failure: "abort",
                },
            ],
        })
        .unwrap_or_default()
    }

    fn initialize(config: String) -> Result<(), String> {
        let value: serde_json::Value = serde_json::from_str(&config).map_err(|e| e.to_string())?;
        let config = if value.is_null() {
            ImageViewerConfig::default()
        } else {
            serde_json::from_value(value).map_err(|e| e.to_string())?
        };
        let max_size = config
            .max_file_size_bytes
            .unwrap_or(DEFAULT_MAX_FILE_SIZE_BYTES);
        MAX_FILE_SIZE_BYTES.with(|current| current.set(max_size));
        Ok(())
    }

    fn invoke(request: String) -> Result<String, String> {
        let request: HookRequest = serde_json::from_str(&request).map_err(|e| e.to_string())?;
        match request.hook.as_str() {
            AGENT_PREPARE_HOOK => {
                let mut draft = request.payload;
                let tools = draft
                    .get_mut("tools")
                    .and_then(serde_json::Value::as_array_mut)
                    .ok_or("agent.prepare payload has no tools array")?;

                tools.push(serde_json::json!({
                    "enabled": true,
                    "name": "view_image",
                    "description": "查看本地图片信息并读取图片内容（支持符合 OpenAI Responses 规范的 PNG, JPEG 及 WEBP 格式）。",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "path": {
                                "type": "string",
                                "description": "本地图片文件路径（支持 png, jpeg, jpg, webp 格式）"
                            },
                            "include_base64": {
                                "type": "boolean",
                                "default": true,
                                "description": "是否在输出中附带 base64 编码的图片数据以供多模态模型查看。默认为 true；若只需要获取图片尺寸/大小等元数据可设为 false"
                            }
                        },
                        "required": ["path"]
                    }
                }));

                Ok(serde_json::json!({"action": "continue", "payload": draft}).to_string())
            }
            TOOLS_CALL_HOOK => call_tool(request.payload),
            hook => Err(format!("unsupported hook: {hook}")),
        }
    }

    fn shutdown() {}
}

fn max_file_size_bytes() -> u64 {
    MAX_FILE_SIZE_BYTES.with(Cell::get)
}

fn call_tool(payload: serde_json::Value) -> Result<String, String> {
    let call: ToolCallRequest = serde_json::from_value(payload).map_err(|e| e.to_string())?;
    let result = match call.name.as_str() {
        "view_image" => match serde_json::from_value::<ViewImageArgs>(call.arguments) {
            Ok(args) => handle_view_image(&args.path, args.include_base64),
            Err(e) => ToolResult {
                success: false,
                output: ToolOutputPayload::Text(format!("invalid arguments for view_image: {e}")),
                error: Some(e.to_string()),
            },
        },
        unknown => return Err(format!("unknown tool: {unknown}")),
    };

    serde_json::to_string(&serde_json::json!({"action": "continue", "payload": result}))
        .map_err(|e| e.to_string())
}

enum SupportedImageFormat {
    Png,
    Jpeg,
    Webp,
}

impl SupportedImageFormat {
    fn mime_type(&self) -> &'static str {
        match self {
            Self::Png => "image/png",
            Self::Jpeg => "image/jpeg",
            Self::Webp => "image/webp",
        }
    }
}

fn detect_supported_format(bytes: &[u8]) -> Result<SupportedImageFormat, String> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        return Ok(SupportedImageFormat::Png);
    }
    if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return Ok(SupportedImageFormat::Jpeg);
    }
    if bytes.starts_with(b"RIFF") && bytes.len() >= 12 && &bytes[8..12] == b"WEBP" {
        return Ok(SupportedImageFormat::Webp);
    }

    // 针对其他常见但在 Responses 图像输入中不被支持的格式给出明确提示
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return Err("GIF format is not supported for model image input. Only PNG, JPEG, and WEBP are supported.".into());
    }
    if bytes.starts_with(b"BM") {
        return Err("BMP format is not supported for model image input. Only PNG, JPEG, and WEBP are supported.".into());
    }
    if bytes.starts_with(b"II\x2A\x00") || bytes.starts_with(b"MM\x00\x2A") {
        return Err("TIFF format is not supported for model image input. Only PNG, JPEG, and WEBP are supported.".into());
    }
    if bytes.starts_with(&[0x00, 0x00, 0x01, 0x00]) {
        return Err("ICO format is not supported for model image input. Only PNG, JPEG, and WEBP are supported.".into());
    }
    if let Ok(s) = std::str::from_utf8(bytes) {
        if s.to_ascii_lowercase().contains("<svg") {
            return Err("SVG format is not supported for model image input. Only PNG, JPEG, and WEBP are supported.".into());
        }
    }

    Err("unrecognized or unsupported image format. Supported formats: PNG, JPEG, WEBP.".into())
}

fn format_file_size(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.2} KB ({bytes} bytes)", bytes as f64 / 1024.0)
    } else {
        format!("{:.2} MB ({bytes} bytes)", bytes as f64 / (1024.0 * 1024.0))
    }
}

fn handle_view_image(path_str: &str, include_base64: bool) -> ToolResult {
    let path = Path::new(path_str);
    let metadata = match fs::metadata(path) {
        Ok(m) => m,
        Err(e) => {
            return ToolResult {
                success: false,
                output: ToolOutputPayload::Text(format!(
                    "failed to read image file metadata for '{path_str}': {e}"
                )),
                error: Some(e.to_string()),
            };
        }
    };

    if metadata.is_dir() {
        return ToolResult {
            success: false,
            output: ToolOutputPayload::Text(format!(
                "target path '{path_str}' is a directory, not an image file"
            )),
            error: Some("target is a directory".to_string()),
        };
    }

    let file_len = metadata.len();
    let max_allowed = max_file_size_bytes();
    if file_len > max_allowed {
        return ToolResult {
            success: false,
            output: ToolOutputPayload::Text(format!(
                "image file '{path_str}' is too large ({file_len} bytes, exceeds limit of {max_allowed} bytes)"
            )),
            error: Some("file too large".to_string()),
        };
    }

    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(e) => {
            return ToolResult {
                success: false,
                output: ToolOutputPayload::Text(format!(
                    "failed to read image file '{path_str}': {e}"
                )),
                error: Some(e.to_string()),
            };
        }
    };

    let format = match detect_supported_format(&bytes) {
        Ok(f) => f,
        Err(e) => {
            return ToolResult {
                success: false,
                output: ToolOutputPayload::Text(format!(
                    "invalid or unsupported image in '{path_str}': {e}"
                )),
                error: Some(e),
            };
        }
    };

    let dimension_info = match imagesize::blob_size(&bytes) {
        Ok(size) => format!("{} x {}", size.width, size.height),
        Err(e) => {
            return ToolResult {
                success: false,
                output: ToolOutputPayload::Text(format!(
                    "failed to decode image dimensions for '{path_str}': {e}"
                )),
                error: Some(format!("image decode error: {e}")),
            };
        }
    };

    let mime_type = format.mime_type();
    let size_info = format_file_size(bytes.len());

    let summary_text = format!(
        "Image Metadata:\n- Path: {}\n- Format: {}\n- Dimensions: {}\n- File Size: {}",
        path_str, mime_type, dimension_info, size_info
    );

    if include_base64 {
        let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
        let data_url = format!("data:{};base64,{}", mime_type, b64);

        let structured_content = vec![
            MessageContentPart::InputText { text: summary_text },
            MessageContentPart::InputImage {
                image_url: Some(data_url),
            },
        ];

        ToolResult {
            success: true,
            output: ToolOutputPayload::Parts(structured_content),
            error: None,
        }
    } else {
        ToolResult {
            success: true,
            output: ToolOutputPayload::Text(summary_text),
            error: None,
        }
    }
}

export!(ImageViewerExtension);
