use std::io::{self, BufRead, IsTerminal};

use anyhow::{Context, Result, bail};
use anyserve_client::{AnyserveClient, JobRecord, JobState};

mod args;
mod output;

pub(crate) use args::JobCommands;
use args::{JobCancelArgs, JobInspectArgs, JobLsArgs, JobStateFilter, OutputFormat};
use output::{
    cancel_result_from_error, cancel_result_from_job, print_job_inspect_text, print_job_list_text,
    print_single_cancel_text, write_cancel_result, write_job_inspect_json, write_job_list_json,
};

pub(crate) async fn run_job_command(endpoint: &str, command: JobCommands) -> Result<()> {
    let mut client = AnyserveClient::connect(endpoint.to_string()).await?;
    match command {
        JobCommands::Ls(args) => run_job_ls(endpoint, &mut client, args).await,
        JobCommands::Inspect(args) => run_job_inspect(endpoint, &mut client, args).await,
        JobCommands::Cancel(args) => run_job_cancel(endpoint, &mut client, args).await,
    }
}

async fn run_job_ls(endpoint: &str, client: &mut AnyserveClient, args: JobLsArgs) -> Result<()> {
    let jobs = client.list_jobs().await?;
    let filtered_jobs = filter_jobs_by_state(jobs, &args.states);

    if args.output.output == OutputFormat::Json {
        return write_job_list_json(&filtered_jobs);
    }

    print_job_list_text(endpoint, &filtered_jobs, &args.states);
    Ok(())
}

async fn run_job_inspect(
    endpoint: &str,
    client: &mut AnyserveClient,
    args: JobInspectArgs,
) -> Result<()> {
    let job = client.get_job(args.job_id.clone()).await?;
    let attempts = client.list_attempts(args.job_id.clone()).await?;
    let streams = client.list_streams(args.job_id).await?;

    if args.output.output == OutputFormat::Json {
        return write_job_inspect_json(&job, &attempts, &streams);
    }

    print_job_inspect_text(endpoint, &job, &attempts, &streams);
    Ok(())
}

async fn run_job_cancel(
    endpoint: &str,
    client: &mut AnyserveClient,
    args: JobCancelArgs,
) -> Result<()> {
    let job_ids = collect_cancel_job_ids(&args)?;
    let batch_mode = args.stdin || job_ids.len() > 1;

    if args.output.output == OutputFormat::Text && !batch_mode {
        let job = client.cancel_job(job_ids[0].clone()).await?;
        print_single_cancel_text(endpoint, &job);
        return Ok(());
    }

    let mut failures = 0usize;
    for job_id in job_ids {
        let result = match client.cancel_job(job_id.clone()).await {
            Ok(job) => cancel_result_from_job(job),
            Err(error) => {
                failures += 1;
                cancel_result_from_error(job_id, error.to_string())
            }
        };

        write_cancel_result(args.output.output, &result)?;
    }

    if failures > 0 {
        bail!("failed to cancel {failures} job(s)");
    }

    Ok(())
}

fn collect_cancel_job_ids(args: &JobCancelArgs) -> Result<Vec<String>> {
    if args.stdin {
        return read_job_ids_from_stdin();
    }

    let job_ids = args
        .job_ids
        .iter()
        .filter_map(|job_id| normalize_job_id(job_id))
        .collect::<Vec<_>>();
    if job_ids.is_empty() {
        bail!("no job ids provided");
    }
    Ok(job_ids)
}

fn read_job_ids_from_stdin() -> Result<Vec<String>> {
    let stdin = io::stdin();
    if stdin.is_terminal() {
        bail!("--stdin requires piped input");
    }
    read_job_ids_from_reader(stdin.lock())
}

fn read_job_ids_from_reader<R: BufRead>(reader: R) -> Result<Vec<String>> {
    let mut job_ids = Vec::new();
    for line in reader.lines() {
        let line = line.context("read stdin line")?;
        if let Some(job_id) = normalize_job_id(&line) {
            job_ids.push(job_id);
        }
    }

    if job_ids.is_empty() {
        bail!("no job ids provided");
    }

    Ok(job_ids)
}

fn normalize_job_id(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn filter_jobs_by_state(jobs: Vec<JobRecord>, states: &[JobStateFilter]) -> Vec<JobRecord> {
    if states.is_empty() {
        return jobs;
    }

    jobs.into_iter()
        .filter(|job| states.iter().any(|state| job_matches_state(job, *state)))
        .collect()
}

fn job_matches_state(job: &JobRecord, state: JobStateFilter) -> bool {
    matches!(
        (job.state(), state),
        (JobState::Pending, JobStateFilter::Pending)
            | (JobState::Leased, JobStateFilter::Leased)
            | (JobState::Running, JobStateFilter::Running)
            | (JobState::Succeeded, JobStateFilter::Succeeded)
            | (JobState::Failed, JobStateFilter::Failed)
            | (JobState::Cancelled, JobStateFilter::Cancelled)
    )
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::{JobStateFilter, filter_jobs_by_state, read_job_ids_from_reader};
    use anyserve_client::{JobRecord, JobState};

    #[test]
    fn read_job_ids_from_reader_trims_and_skips_empty_lines() {
        let input = Cursor::new("job-1\n\n  job-2 \n   \njob-3\n");
        let job_ids = read_job_ids_from_reader(input).unwrap();
        assert_eq!(job_ids, vec!["job-1", "job-2", "job-3"]);
    }

    #[test]
    fn filter_jobs_by_state_keeps_only_requested_states() {
        let pending = JobRecord {
            job_id: "job-pending".to_string(),
            state: JobState::Pending as i32,
            ..JobRecord::default()
        };
        let cancelled = JobRecord {
            job_id: "job-cancelled".to_string(),
            state: JobState::Cancelled as i32,
            ..JobRecord::default()
        };

        let filtered =
            filter_jobs_by_state(vec![pending.clone(), cancelled], &[JobStateFilter::Pending]);

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].job_id, pending.job_id);
    }
}
