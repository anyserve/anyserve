use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use anyserve_client::{AnyserveClient, EventKind, JobRecord};
use futures::StreamExt;

use crate::serve_openai_worker::config::ResolvedServeOpenAIWorkerConfig;
use crate::serve_openai_worker::helpers::{
    DEFAULT_OUTPUT_STREAM, frame_write_with_content_type, open_output_stream, persisted_outputs,
    read_request_body, truncate, upstream_path,
};

pub(super) async fn process_grant(
    client: &mut AnyserveClient,
    http_client: &reqwest::Client,
    config: &ResolvedServeOpenAIWorkerConfig,
    worker_id: &str,
    job: JobRecord,
) -> Result<()> {
    let lease_id = job.lease_id.clone();
    if lease_id.trim().is_empty() {
        bail!("job '{}' is missing lease_id", job.job_id);
    }

    let attempt_id = job.current_attempt_id.clone().trim().to_string();
    let spec = job
        .spec
        .clone()
        .context("job is missing spec for llm worker")?;
    let upstream_path = upstream_path(&spec.interface_name)?;
    let request_body =
        read_request_body(client, &job.job_id, &spec.params, config.input_stream_wait).await?;
    if request_body.is_empty() {
        bail!("job '{}' request body was empty", job.job_id);
    }

    client
        .report_event(
            worker_id.to_string(),
            lease_id.clone(),
            EventKind::Started,
            Vec::new(),
            HashMap::from([
                ("provider".to_string(), config.provider.clone()),
                ("worker.kind".to_string(), config.kind.clone()),
            ]),
        )
        .await?;

    let response = http_client
        .post(format!("{}/{}", config.base_url, upstream_path))
        .body(request_body)
        .send()
        .await
        .with_context(|| format!("proxy request to {}", config.base_url))?;

    let status = response.status();
    if !status.is_success() {
        return fail_upstream_request(client, worker_id, lease_id, config, status, response).await;
    }

    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json")
        .to_string();

    let output_stream = client
        .open_stream(open_output_stream(
            job.job_id.clone(),
            worker_id.to_string(),
            lease_id.clone(),
            attempt_id,
        ))
        .await?;

    let persisted_output = persist_upstream_response(
        client,
        output_stream.stream_id.clone(),
        worker_id,
        &lease_id,
        &content_type,
        response,
    )
    .await?;

    let outputs = persisted_outputs(&content_type, persisted_output);

    client
        .close_stream(
            output_stream.stream_id.clone(),
            Some(worker_id.to_string()),
            Some(lease_id.clone()),
            HashMap::new(),
        )
        .await?;
    client
        .report_event(
            worker_id.to_string(),
            lease_id.clone(),
            EventKind::OutputReady,
            Vec::new(),
            HashMap::from([
                ("stream_name".to_string(), DEFAULT_OUTPUT_STREAM.to_string()),
                ("content_type".to_string(), content_type),
            ]),
        )
        .await?;
    client
        .complete_lease(worker_id.to_string(), lease_id, outputs, HashMap::new())
        .await?;

    Ok(())
}

async fn fail_upstream_request(
    client: &mut AnyserveClient,
    worker_id: &str,
    lease_id: String,
    config: &ResolvedServeOpenAIWorkerConfig,
    status: reqwest::StatusCode,
    response: reqwest::Response,
) -> Result<()> {
    let body = response.text().await.unwrap_or_default();
    client
        .fail_lease(
            worker_id.to_string(),
            lease_id,
            format!("upstream returned {}: {}", status, truncate(&body, 512)),
            false,
            HashMap::from([
                ("provider".to_string(), config.provider.clone()),
                ("worker.kind".to_string(), config.kind.clone()),
            ]),
        )
        .await?;
    Ok(())
}

async fn persist_upstream_response(
    client: &mut AnyserveClient,
    stream_id: String,
    worker_id: &str,
    lease_id: &str,
    content_type: &str,
    response: reqwest::Response,
) -> Result<Vec<u8>> {
    let mut persisted_output = Vec::new();
    if content_type.contains("text/event-stream") {
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.context("read upstream event stream chunk")?;
            if chunk.is_empty() {
                continue;
            }
            persisted_output.extend_from_slice(&chunk);
            client
                .push_frames(
                    stream_id.clone(),
                    vec![frame_write_with_content_type(
                        chunk.to_vec(),
                        content_type.to_string(),
                    )],
                    Some(worker_id.to_string()),
                    Some(lease_id.to_string()),
                )
                .await?;
        }
    } else {
        let body = response
            .bytes()
            .await
            .context("read upstream response body")?;
        persisted_output = body.to_vec();
        client
            .push_frames(
                stream_id,
                vec![frame_write_with_content_type(
                    body.to_vec(),
                    content_type.to_string(),
                )],
                Some(worker_id.to_string()),
                Some(lease_id.to_string()),
            )
            .await?;
    }

    Ok(persisted_output)
}
