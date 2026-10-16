use crate::ClientError;
use chrono::Utc;
use eventsource_stream::Eventsource;
use futures::{Stream, StreamExt};
use ragents_protocol::*;
use reqwest::{Client, StatusCode};
use std::collections::BTreeMap;
use std::pin::Pin;

#[derive(Clone)]
pub struct RagentsClient {
    base_url: String,
    http: Client,
}

impl RagentsClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        let raw = base_url.into();
        let trimmed = raw.trim_end_matches('/').to_string();
        Self {
            base_url: trimmed,
            http: Client::new(),
        }
    }

    pub fn with_auth_token(mut self, auth_token: Option<String>) -> Self {
        if let Some(tok) = auth_token {
            let mut headers = reqwest::header::HeaderMap::new();
            if let Ok(val) = reqwest::header::HeaderValue::from_str(&format!("Bearer {tok}")) {
                headers.insert(reqwest::header::AUTHORIZATION, val);
                if let Ok(client) = Client::builder().default_headers(headers).build() {
                    self.http = client;
                }
            }
        }
        self
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn ws_tunnel_url(&self) -> String {
        let ws_base = if let Some(rest) = self.base_url.strip_prefix("https://") {
            format!("wss://{rest}")
        } else if let Some(rest) = self.base_url.strip_prefix("http://") {
            format!("ws://{rest}")
        } else {
            format!("ws://{}", self.base_url)
        };
        format!("{ws_base}/api/v1/daemon/tunnel")
    }

    async fn parse_response<T: serde::de::DeserializeOwned>(
        resp: reqwest::Response,
    ) -> Result<T, ClientError> {
        let status = resp.status();
        if !status.is_success() {
            let status_code = status.as_u16();
            let text = resp.text().await.unwrap_or_default();
            let message = serde_json::from_str::<serde_json::Value>(&text)
                .ok()
                .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(String::from))
                .unwrap_or(text);
            return Err(ClientError::Api {
                status: status_code,
                message,
            });
        }
        Ok(resp.json::<T>().await?)
    }

    pub async fn list_nodes(&self) -> Result<Vec<Node>, ClientError> {
        let url = format!("{}/api/v1/nodes", self.base_url);
        let resp = self.http.get(url).send().await?;
        Self::parse_response(resp).await
    }

    pub async fn get_node(&self, node_id: &NodeId) -> Result<Option<Node>, ClientError> {
        let url = format!("{}/api/v1/nodes/{}", self.base_url, node_id);
        let resp = self.http.get(url).send().await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Self::parse_response(resp).await.map(Some)
    }

    pub async fn create_session(
        &self,
        req: CreateSessionRequest,
    ) -> Result<CreateSessionResponse, ClientError> {
        let url = format!("{}/api/v1/sessions", self.base_url);
        let resp = self.http.post(url).json(&req).send().await?;
        Self::parse_response(resp).await
    }

    pub async fn list_sessions(&self) -> Result<Vec<Session>, ClientError> {
        let url = format!("{}/api/v1/sessions", self.base_url);
        let resp = self.http.get(url).send().await?;
        Self::parse_response(resp).await
    }

    pub async fn get_session(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<Session>, ClientError> {
        let url = format!("{}/api/v1/sessions/{}", self.base_url, session_id);
        let resp = self.http.get(url).send().await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Self::parse_response(resp).await.map(Some)
    }

    pub async fn patch_session(
        &self,
        session_id: &SessionId,
        patch: PatchSessionSpecRequest,
    ) -> Result<Session, ClientError> {
        let url = format!("{}/api/v1/sessions/{}", self.base_url, session_id);
        let resp = self.http.patch(url).json(&patch).send().await?;
        Self::parse_response(resp).await
    }

    pub async fn get_session_context(
        &self,
        session_id: &SessionId,
    ) -> Result<SessionContextResponse, ClientError> {
        let url = format!("{}/api/v1/sessions/{}/context", self.base_url, session_id);
        let resp = self.http.get(url).send().await?;
        Self::parse_response(resp).await
    }

    pub async fn list_session_activations(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<Activation>, ClientError> {
        let url = format!(
            "{}/api/v1/sessions/{}/activations",
            self.base_url, session_id
        );
        let resp = self.http.get(url).send().await?;
        Self::parse_response(resp).await
    }

    pub async fn create_activation(
        &self,
        req: CreateActivationRequest,
    ) -> Result<Activation, ClientError> {
        let url = format!("{}/api/v1/activations", self.base_url);
        let resp = self.http.post(url).json(&req).send().await?;
        Self::parse_response(resp).await
    }

    pub async fn push_realtime_message(
        &self,
        session_id: &SessionId,
        req: PushRealtimeMessageRequest,
    ) -> Result<PushRealtimeMessageResponse, ClientError> {
        let url = format!("{}/api/v1/sessions/{}/realtime", self.base_url, session_id);
        let resp = self.http.post(url).json(&req).send().await?;
        Self::parse_response(resp).await
    }

    pub async fn get_activation(
        &self,
        activation_id: &ActivationId,
    ) -> Result<Option<Activation>, ClientError> {
        let url = format!("{}/api/v1/activations/{}", self.base_url, activation_id);
        let resp = self.http.get(url).send().await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Self::parse_response(resp).await.map(Some)
    }

    pub async fn patch_activation(
        &self,
        activation_id: &ActivationId,
        patch: PatchActivationSpecRequest,
    ) -> Result<Activation, ClientError> {
        let url = format!("{}/api/v1/activations/{}", self.base_url, activation_id);
        let resp = self.http.patch(url).json(&patch).send().await?;
        Self::parse_response(resp).await
    }

    pub async fn cancel_activation(
        &self,
        activation_id: &ActivationId,
    ) -> Result<Activation, ClientError> {
        self.patch_activation(
            activation_id,
            PatchActivationSpecRequest {
                desired_phase: Some(DesiredActivationPhase::Cancelled),
                interaction_responses: BTreeMap::new(),
            },
        )
        .await
    }

    pub async fn respond_permission(
        &self,
        activation_id: &ActivationId,
        request_id: PermissionRequestId,
        decision: PermissionDecision,
    ) -> Result<Activation, ClientError> {
        self.respond_permission_with_path(activation_id, request_id, decision, None)
            .await
    }

    pub async fn respond_permission_with_path(
        &self,
        activation_id: &ActivationId,
        request_id: PermissionRequestId,
        decision: PermissionDecision,
        override_path: Option<std::path::PathBuf>,
    ) -> Result<Activation, ClientError> {
        let mut interaction_responses = BTreeMap::new();
        interaction_responses.insert(
            request_id,
            PermissionResolutionSpec {
                decision,
                override_path,
                responded_at: Utc::now(),
            },
        );
        self.patch_activation(
            activation_id,
            PatchActivationSpecRequest {
                desired_phase: None,
                interaction_responses,
            },
        )
        .await
    }

    pub async fn read_events(
        &self,
        session_id: Option<&SessionId>,
        after_rv: u64,
    ) -> Result<Vec<SessionEventEnvelope>, ClientError> {
        let mut url = format!("{}/api/v1/events?after_rv={}", self.base_url, after_rv);
        if let Some(sess_id) = session_id {
            url.push_str(&format!("&session_id={sess_id}"));
        }
        let resp = self.http.get(url).send().await?;
        Self::parse_response(resp).await
    }

    pub async fn watch_events(
        &self,
        session_id: Option<&SessionId>,
        after_rv: u64,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<SessionEventEnvelope, ClientError>> + Send>>,
        ClientError,
    > {
        let mut url = format!("{}/api/v1/watch?after_rv={}", self.base_url, after_rv);
        if let Some(sess_id) = session_id {
            url.push_str(&format!("&session_id={sess_id}"));
        }
        let resp = self.http.get(url).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(ClientError::Api {
                status: status.as_u16(),
                message: text,
            });
        }

        let byte_stream = resp.bytes_stream().eventsource();
        let mapped = byte_stream.filter_map(|item| async move {
            match item {
                Ok(ev) => {
                    if ev.data.trim().is_empty() {
                        None
                    } else {
                        Some(
                            serde_json::from_str::<SessionEventEnvelope>(&ev.data)
                                .map_err(ClientError::Json),
                        )
                    }
                }
                Err(e) => Some(Err(ClientError::Tunnel(format!("sse stream error: {e}")))),
            }
        });

        Ok(Box::pin(mapped))
    }
}
