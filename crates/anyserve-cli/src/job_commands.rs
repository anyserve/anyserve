use std::io::{self, BufRead, IsTerminal};

use anyhow::{Context, Result, bail};
use anyserve_client::execution::{ExecutionClient, v2};
use serde_json::{Value, json};

mod args;
pub(crate) use args::JobCommands;
use args::OutputFormat;

pub(crate) async fn run_job_command(endpoint: &str, command: JobCommands) -> Result<()> {
    let mut client = ExecutionClient::connect(endpoint).await?;
    match command {
        JobCommands::Ls(args) => {
            let page = client
                .list(v2::ListJobsRequest {
                    states: args
                        .states
                        .into_iter()
                        .map(|state| state.protocol())
                        .collect(),
                    limit: args.limit,
                    page_token: args.page_token.unwrap_or_default(),
                })
                .await?;
            if args.output.output == OutputFormat::Json {
                println!(
                    "{}",
                    json!({ "jobs": page.jobs.iter().map(job_json).collect::<Vec<_>>(), "next_page_token": page.next_page_token })
                );
            } else {
                println!(
                    "{:<38} {:<11} {:<24} {:<8} WORKER",
                    "JOB", "STATE", "INTERFACE", "ATTEMPTS"
                );
                for job in &page.jobs {
                    println!(
                        "{:<38} {:<11} {:<24} {:<8} {}",
                        job.job_id,
                        state_name(job.state),
                        job.interface,
                        job.attempt_count,
                        job.current_worker_id
                    );
                }
                if !page.next_page_token.is_empty() {
                    eprintln!("Next page: --page-token {}", page.next_page_token);
                }
            }
        }
        JobCommands::Inspect(args) => {
            let job = client.get(args.job_id, true).await?;
            if args.output.output == OutputFormat::Json {
                println!("{}", job_json(&job));
            } else {
                println!("{}", serde_json::to_string_pretty(&job_json(&job))?);
            }
        }
        JobCommands::Cancel(args) => {
            let ids = if args.stdin {
                let stdin = io::stdin();
                if stdin.is_terminal() {
                    bail!("--stdin requires piped input");
                }
                read_job_ids(stdin.lock())?
            } else {
                args.job_ids
            };
            let mut failures = 0;
            for id in ids {
                match client.cancel(id.clone(), "cancelled from CLI").await {
                    Ok(job) if args.output.output == OutputFormat::Json => {
                        println!("{}", job_json(&job))
                    }
                    Ok(job) => println!("{} {}", job.job_id, state_name(job.state)),
                    Err(error) => {
                        failures += 1;
                        println!("{}", json!({ "job_id": id, "error": error.message() }));
                    }
                }
            }
            if failures > 0 {
                bail!("failed to cancel {failures} job(s)");
            }
        }
    }
    Ok(())
}

fn job_json(job: &v2::Job) -> Value {
    json!({ "job_id": job.job_id, "state": state_name(job.state),
        "durability": if job.durability == v2::Durability::Durable as i32 { "durable" } else { "ephemeral" },
        "interface": job.interface, "attempt_count": job.attempt_count,
        "current_attempt_id": job.current_attempt_id, "current_worker_id": job.current_worker_id,
        "created_at_ms": job.created_at_ms, "updated_at_ms": job.updated_at_ms,
        "error": job.error.as_ref().map(|error| json!({ "code": error.code, "message": error.message, "retryable": error.retryable })),
        "metadata": job.metadata, "spec_hash": job.spec_hash,
        "outputs": job.outputs.iter().map(|object| match &object.body {
            Some(v2::object::Body::Inline(bytes)) => json!({"inline_bytes": bytes.len(), "metadata": object.metadata}),
            Some(v2::object::Body::Uri(uri)) => json!({"uri": uri, "metadata": object.metadata}),
            None => Value::Null,
        }).collect::<Vec<_>>() })
}
fn state_name(state: i32) -> &'static str {
    match v2::JobState::try_from(state) {
        Ok(v2::JobState::Queued) => "queued",
        Ok(v2::JobState::Running) => "running",
        Ok(v2::JobState::Succeeded) => "succeeded",
        Ok(v2::JobState::Failed) => "failed",
        Ok(v2::JobState::Cancelled) => "cancelled",
        _ => "unspecified",
    }
}
fn read_job_ids(reader: impl BufRead) -> Result<Vec<String>> {
    let ids = reader
        .lines()
        .map(|line| line.context("read stdin line"))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(|line| line.trim().to_owned())
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    if ids.is_empty() {
        bail!("no job ids provided");
    }
    Ok(ids)
}
#[cfg(test)]
mod tests {
    #[test]
    fn stdin_job_ids_skip_blank_lines() {
        let ids = super::read_job_ids(std::io::Cursor::new("job-1\n\n  job-2 \n")).unwrap();
        assert_eq!(ids, vec!["job-1", "job-2"]);
    }
}
