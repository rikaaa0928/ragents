use async_trait::async_trait;
use axum::http::{HeaderMap, StatusCode};
use ragents_protocol::*;
use std::collections::BTreeMap;
use std::sync::Arc;

#[async_trait]
pub trait AuthnPlugin: Send + Sync {
    fn id(&self) -> &str;

    fn scopes(&self) -> &[AuthScope] {
        &[AuthScope::ClientHttp, AuthScope::DaemonTunnel]
    }

    async fn authenticate(&self, req: &AuthnRequest) -> Result<AuthnResponse, String>;
}

/// Built-in constant-time static Bearer token authenticator for `api-server`.
#[derive(Debug, Clone)]
pub struct StaticTokenAuthnPlugin {
    token: String,
    subject: String,
    tenant_id: String,
}

impl StaticTokenAuthnPlugin {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
            subject: "authenticated-client".to_string(),
            tenant_id: "default".to_string(),
        }
    }
}

fn constant_time_eq_bytes(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (&x, &y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[async_trait]
impl AuthnPlugin for StaticTokenAuthnPlugin {
    fn id(&self) -> &str {
        "static-token-authn"
    }

    async fn authenticate(&self, req: &AuthnRequest) -> Result<AuthnResponse, String> {
        let mut candidate: Option<&str> = None;
        for (k, v) in &req.headers {
            if k.eq_ignore_ascii_case("authorization") {
                let trimmed = v.trim();
                if let Some(rest) = trimmed
                    .strip_prefix("Bearer ")
                    .or_else(|| trimmed.strip_prefix("bearer "))
                {
                    let tok = rest.trim();
                    if !tok.is_empty() {
                        candidate = Some(tok);
                        break;
                    }
                } else if !trimmed.is_empty() {
                    candidate = Some(trimmed);
                    break;
                }
            } else if k.eq_ignore_ascii_case("x-ragents-token") {
                let trimmed = v.trim();
                if !trimmed.is_empty() {
                    candidate = Some(trimmed);
                    break;
                }
            }
        }

        let Some(cand) = candidate else {
            return Ok(AuthnResponse::Unauthenticated {
                message: "Missing Authorization Bearer token".to_string(),
            });
        };

        if constant_time_eq_bytes(cand.as_bytes(), self.token.as_bytes()) {
            let kind = match req.scope {
                AuthScope::DaemonTunnel => PrincipalKind::DaemonNode,
                AuthScope::ClientHttp => PrincipalKind::ClientUser,
            };
            Ok(AuthnResponse::Authenticated {
                principal: Principal {
                    kind,
                    subject: self.subject.clone(),
                    tenant_id: self.tenant_id.clone(),
                    roles: vec!["admin".to_string()],
                    claims: BTreeMap::new(),
                },
            })
        } else {
            Ok(AuthnResponse::Unauthenticated {
                message: "Invalid Bearer token".to_string(),
            })
        }
    }
}

#[derive(Default, Clone)]
pub struct ApiServerExtensions {
    authn_plugins: Vec<Arc<dyn AuthnPlugin>>,
}

impl ApiServerExtensions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_authn(&mut self, plugin: Arc<dyn AuthnPlugin>) {
        self.authn_plugins.push(plugin);
    }

    pub async fn authenticate(
        &self,
        scope: AuthScope,
        method: &str,
        path: &str,
        headers: &HeaderMap,
    ) -> Result<Principal, (StatusCode, String)> {
        let applicable: Vec<&Arc<dyn AuthnPlugin>> = self
            .authn_plugins
            .iter()
            .filter(|p| p.scopes().contains(&scope))
            .collect();

        if applicable.is_empty() {
            return Ok(Principal {
                kind: match scope {
                    AuthScope::DaemonTunnel => PrincipalKind::DaemonNode,
                    AuthScope::ClientHttp => PrincipalKind::Anonymous,
                },
                subject: "anonymous".to_string(),
                tenant_id: "default".to_string(),
                roles: vec!["admin".to_string()],
                claims: BTreeMap::new(),
            });
        }

        let mut header_map = BTreeMap::new();
        for (k, v) in headers.iter() {
            if let Ok(val) = v.to_str() {
                header_map.insert(k.as_str().to_ascii_lowercase(), val.to_string());
            }
        }

        let req = AuthnRequest {
            scope,
            method: method.to_string(),
            path: path.to_string(),
            headers: header_map,
        };

        let mut last_unauth_msg = "Unauthorized".to_string();
        for plugin in applicable {
            match plugin.authenticate(&req).await {
                Ok(AuthnResponse::Authenticated { principal }) => return Ok(principal),
                Ok(AuthnResponse::Abstain) => continue,
                Ok(AuthnResponse::Unauthenticated { message }) => {
                    last_unauth_msg = message;
                }
                Err(err) => {
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("authn plugin '{}' failed: {err}", plugin.id()),
                    ));
                }
            }
        }

        Err((StatusCode::UNAUTHORIZED, last_unauth_msg))
    }
}
