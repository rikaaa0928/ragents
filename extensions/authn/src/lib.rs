use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Mutex;

wit_bindgen::generate!({
    path: "../../wit",
    world: "plugin",
});

#[derive(Debug, Clone, Deserialize, Default)]
struct AuthnConfig {
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    subject: Option<String>,
    #[serde(default)]
    tenant_id: Option<String>,
    #[serde(default)]
    tokens: Vec<TokenEntry>,
}

#[derive(Debug, Clone, Deserialize)]
struct TokenEntry {
    token: String,
    #[serde(default = "default_subject")]
    subject: String,
    #[serde(default = "default_tenant")]
    tenant_id: String,
    #[serde(default)]
    scopes: Vec<String>,
    #[serde(default)]
    roles: Vec<String>,
    #[serde(default)]
    claims: BTreeMap<String, String>,
}

fn default_subject() -> String {
    "authenticated-client".to_string()
}

fn default_tenant() -> String {
    "default".to_string()
}

static CONFIG: Mutex<Option<AuthnConfig>> = Mutex::new(None);

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
    payload: Value,
}

#[derive(Deserialize)]
struct AuthnRequestPayload {
    scope: String,
    #[serde(default)]
    headers: BTreeMap<String, String>,
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();
    if a_bytes.len() != b_bytes.len() {
        return false;
    }
    let mut diff = 0u8;
    for (&x, &y) in a_bytes.iter().zip(b_bytes.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn extract_bearer_token(headers: &BTreeMap<String, String>) -> Option<String> {
    for (k, v) in headers {
        if k.eq_ignore_ascii_case("authorization") {
            let trimmed = v.trim();
            if let Some(rest) = trimmed
                .strip_prefix("Bearer ")
                .or_else(|| trimmed.strip_prefix("bearer "))
            {
                let tok = rest.trim();
                if !tok.is_empty() {
                    return Some(tok.to_string());
                }
            } else if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        } else if k.eq_ignore_ascii_case("x-ragents-token") {
            let trimmed = v.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

struct AuthnExtension;

impl exports::ragent::extension::lifecycle::Guest for AuthnExtension {
    fn metadata() -> String {
        serde_json::to_string(&Metadata {
            id: "authn",
            version: env!("CARGO_PKG_VERSION"),
            subscriptions: vec![Subscription {
                hook: "authn",
                kind: "action",
                priority: 10,
                failure: "abort",
            }],
        })
        .unwrap_or_default()
    }

    fn initialize(config_json: String) -> Result<(), String> {
        let cfg = if config_json.trim().is_empty() || config_json.trim() == "null" {
            AuthnConfig::default()
        } else {
            serde_json::from_str::<AuthnConfig>(&config_json).map_err(|e| e.to_string())?
        };
        let mut guard = CONFIG.lock().map_err(|e| e.to_string())?;
        *guard = Some(cfg);
        Ok(())
    }

    fn invoke(request_json: String) -> Result<String, String> {
        let req: HookRequest = serde_json::from_str(&request_json).map_err(|e| e.to_string())?;
        if req.hook != "authn" {
            return Err(format!("unsupported hook: {}", req.hook));
        }

        let auth_req: AuthnRequestPayload =
            serde_json::from_value(req.payload).map_err(|e| e.to_string())?;
        let guard = CONFIG.lock().map_err(|e| e.to_string())?;
        let cfg = guard.as_ref().cloned().unwrap_or_default();

        let mut entries = cfg.tokens;
        if let Some(single_tok) = cfg.token.filter(|s| !s.trim().is_empty()) {
            entries.push(TokenEntry {
                token: single_tok,
                subject: cfg.subject.unwrap_or_else(default_subject),
                tenant_id: cfg.tenant_id.unwrap_or_else(default_tenant),
                scopes: vec![],
                roles: vec!["admin".to_string()],
                claims: BTreeMap::new(),
            });
        }

        if entries.is_empty() {
            return Ok(json!({
                "ok": true,
                "output": {
                    "status": "unauthenticated",
                    "message": "Authn plugin has no configured tokens"
                }
            })
            .to_string());
        }

        let Some(candidate) = extract_bearer_token(&auth_req.headers) else {
            return Ok(json!({
                "ok": true,
                "output": {
                    "status": "unauthenticated",
                    "message": "Missing Authorization Bearer token"
                }
            })
            .to_string());
        };

        for entry in &entries {
            if constant_time_eq(&candidate, &entry.token) {
                if !entry.scopes.is_empty()
                    && !entry
                        .scopes
                        .iter()
                        .any(|s| s.eq_ignore_ascii_case(&auth_req.scope))
                {
                    return Ok(json!({
                        "ok": true,
                        "output": {
                            "status": "unauthenticated",
                            "message": format!("Token is not authorized for scope '{}'", auth_req.scope)
                        }
                    })
                    .to_string());
                }

                let kind = if auth_req.scope == "daemon_tunnel" {
                    "daemon_node"
                } else {
                    "client_user"
                };

                return Ok(json!({
                    "ok": true,
                    "output": {
                        "status": "authenticated",
                        "principal": {
                            "kind": kind,
                            "subject": entry.subject,
                            "tenant_id": entry.tenant_id,
                            "roles": entry.roles,
                            "claims": entry.claims
                        }
                    }
                })
                .to_string());
            }
        }

        Ok(json!({
            "ok": true,
            "output": {
                "status": "unauthenticated",
                "message": "Invalid Bearer token"
            }
        })
        .to_string())
    }

    fn shutdown() {}
}

export!(AuthnExtension);
