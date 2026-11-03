//! `--refresh-cmd`: obtain a new URL when a signed URL expires (HTTP-12).

use qget_core::{BoxFuture, Validator};
use qget_http::{HttpClient, Refresher};
use std::sync::Arc;

/// Build a refresher that runs `cmd` via `sh -c`, takes the first non-empty
/// line of its output as the new URL, and checks it serves the same object.
pub fn command_refresher(
    cmd: String,
    client: HttpClient,
    size: u64,
    validator: Validator,
) -> Refresher {
    Arc::new(move || -> BoxFuture<'static, Result<String, String>> {
        let (cmd, client, validator) = (cmd.clone(), client.clone(), validator.clone());
        Box::pin(async move {
            let out = tokio::process::Command::new("sh")
                .arg("-c")
                .arg(&cmd)
                .output()
                .await
                .map_err(|e| format!("run refresh command: {e}"))?;
            if !out.status.success() {
                return Err(format!("refresh command exited with {}", out.status));
            }
            let url = String::from_utf8_lossy(&out.stdout)
                .lines()
                .map(str::trim)
                .find(|l| !l.is_empty())
                .ok_or("refresh command printed no URL")?
                .to_string();
            let info = client.probe(&url).await.map_err(|e| e.to_string())?;
            if info.size != Some(size) || validator.same_as(&info.validator) == Some(false) {
                return Err("refreshed URL serves a different object".into());
            }
            tracing::info!("refreshed expired URL");
            Ok(info.url)
        })
    })
}
