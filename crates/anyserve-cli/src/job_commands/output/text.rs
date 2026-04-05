use anyhow::Result;
use anyserve_client::{AttemptRecord, JobRecord, StreamRecord};

use crate::job_commands::args::JobStateFilter;

use super::json::CancelJsonResult;
use super::shared::{
    attempt_state_name, count_jobs, ellipsize, format_age, format_job_counts,
    format_optional_timestamp, format_state_filters, format_string_map, format_timestamp,
    interface_name, job_metadata, job_state_name, optional_text, stream_direction_name,
    stream_scope_name, stream_state_name,
};

pub(super) fn print_job_list_text(endpoint: &str, jobs: &[JobRecord], states: &[JobStateFilter]) {
    let counts = count_jobs(jobs);
    print_section("Control Plane");
    let mut entries = vec![
        ("endpoint", endpoint.to_string()),
        ("jobs", format!("{} total", jobs.len())),
        ("states", format_job_counts(counts)),
    ];
    if !states.is_empty() {
        entries.push(("filter", format_state_filters(states)));
    }
    print_key_values(&entries);
    println!();

    print_section("Jobs");
    if jobs.is_empty() {
        println!("No jobs found.");
        return;
    }

    let rows = jobs
        .iter()
        .map(|job| {
            vec![
                ellipsize(&job.job_id, 18),
                job_state_name(job.state()).to_string(),
                ellipsize(interface_name(job), 24),
                ellipsize(optional_text(&job.current_attempt_id), 18),
                ellipsize(optional_text(&job.last_error), 24),
                format_age(job.updated_at_ms),
            ]
        })
        .collect::<Vec<_>>();
    print_table(
        &[
            "JOB ID",
            "STATE",
            "INTERFACE",
            "ATTEMPT",
            "LAST ERROR",
            "UPDATED",
        ],
        &rows,
    );
}

pub(super) fn print_job_inspect_text(
    endpoint: &str,
    job: &JobRecord,
    attempts: &[AttemptRecord],
    streams: &[StreamRecord],
) {
    print_section("Job");
    print_key_values(&[
        ("endpoint", endpoint.to_string()),
        ("job_id", job.job_id.clone()),
        ("state", job_state_name(job.state()).to_string()),
        ("interface", interface_name(job).to_string()),
        (
            "current_attempt",
            optional_text(&job.current_attempt_id).to_string(),
        ),
        ("lease_id", optional_text(&job.lease_id).to_string()),
        ("created_at", format_timestamp(job.created_at_ms)),
        ("updated_at", format_timestamp(job.updated_at_ms)),
        ("outputs", job.outputs.len().to_string()),
        ("last_error", optional_text(&job.last_error).to_string()),
        ("metadata", format_string_map(job_metadata(job))),
    ]);
    println!();

    print_section("Attempts");
    if attempts.is_empty() {
        println!("No attempts recorded.");
    } else {
        let rows = attempts
            .iter()
            .map(|attempt| {
                vec![
                    ellipsize(&attempt.attempt_id, 18),
                    attempt_state_name(attempt.state()).to_string(),
                    ellipsize(optional_text(&attempt.worker_id), 18),
                    ellipsize(optional_text(&attempt.lease_id), 18),
                    format_age(attempt.created_at_ms),
                    format_optional_timestamp(attempt.finished_at_ms),
                ]
            })
            .collect::<Vec<_>>();
        print_table(
            &[
                "ATTEMPT ID",
                "STATE",
                "WORKER",
                "LEASE",
                "CREATED",
                "FINISHED",
            ],
            &rows,
        );
    }
    println!();

    print_section("Streams");
    if streams.is_empty() {
        println!("No streams recorded.");
    } else {
        let rows = streams
            .iter()
            .map(|stream| {
                vec![
                    ellipsize(&stream.stream_id, 18),
                    ellipsize(&stream.stream_name, 20),
                    stream_scope_name(stream.scope()).to_string(),
                    stream_direction_name(stream.direction()).to_string(),
                    stream_state_name(stream.state()).to_string(),
                    format_optional_timestamp(stream.closed_at_ms),
                ]
            })
            .collect::<Vec<_>>();
        print_table(
            &["STREAM ID", "NAME", "SCOPE", "DIRECTION", "STATE", "CLOSED"],
            &rows,
        );
    }
}

pub(super) fn print_single_cancel_text(endpoint: &str, job: &JobRecord) {
    let state = job_state_name(job.state()).to_string();
    let updated_at = format_timestamp(job.updated_at_ms);

    print_section("Cancel Result");
    print_key_values(&[
        ("endpoint", endpoint.to_string()),
        ("job_id", job.job_id.clone()),
        ("state", state),
        ("updated_at", updated_at),
    ]);
}

pub(super) fn write_cancel_result_text(result: &CancelJsonResult) -> Result<()> {
    print_cancel_result_text(result);
    Ok(())
}

fn print_cancel_result_text(result: &CancelJsonResult) {
    match (
        &result.ok,
        &result.state,
        &result.updated_at_ms,
        &result.error,
    ) {
        (true, Some(state), Some(updated_at_ms), _) => {
            println!(
                "cancelled {} state={} updated_at_ms={}",
                result.job_id, state, updated_at_ms
            );
        }
        (false, _, _, Some(error)) => {
            println!("failed {} error={}", result.job_id, error);
        }
        _ => {
            println!("failed {} error=unknown", result.job_id);
        }
    }
}

fn print_section(title: &str) {
    println!("{title}");
    println!("{}", "=".repeat(title.len()));
}

fn print_key_values(entries: &[(&str, String)]) {
    let width = entries.iter().map(|(key, _)| key.len()).max().unwrap_or(0);
    for (key, value) in entries {
        println!("{key:width$} : {value}", width = width);
    }
}

fn print_table(headers: &[&str], rows: &[Vec<String>]) {
    let mut widths = headers
        .iter()
        .map(|header| header.len())
        .collect::<Vec<_>>();
    for row in rows {
        for (index, cell) in row.iter().enumerate() {
            if let Some(width) = widths.get_mut(index) {
                *width = (*width).max(cell.len());
            }
        }
    }

    let separator = format!(
        "+{}+",
        widths
            .iter()
            .map(|width| "-".repeat(width + 2))
            .collect::<Vec<_>>()
            .join("+")
    );

    println!("{separator}");
    print!("|");
    for (header, width) in headers.iter().zip(widths.iter()) {
        print!(" {:width$} |", header, width = *width);
    }
    println!();
    println!("{separator}");

    for row in rows {
        print!("|");
        for (cell, width) in row.iter().zip(widths.iter()) {
            print!(" {:width$} |", cell, width = *width);
        }
        println!();
    }
    println!("{separator}");
}
