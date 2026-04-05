use clap::{Args, Subcommand, ValueEnum};

#[derive(Subcommand, Debug)]
pub(crate) enum JobCommands {
    #[command(alias = "list")]
    Ls(JobLsArgs),
    Inspect(JobInspectArgs),
    Cancel(JobCancelArgs),
}

#[derive(Args, Clone, Debug)]
pub(crate) struct OutputArgs {
    #[arg(short = 'o', long = "output", value_enum, default_value = "text")]
    pub(crate) output: OutputFormat,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum OutputFormat {
    Text,
    Json,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum JobStateFilter {
    Pending,
    Leased,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Args, Debug)]
pub(crate) struct JobLsArgs {
    #[arg(long = "state", value_enum)]
    pub(crate) states: Vec<JobStateFilter>,
    #[command(flatten)]
    pub(crate) output: OutputArgs,
}

#[derive(Args, Debug)]
pub(crate) struct JobInspectArgs {
    pub(crate) job_id: String,
    #[command(flatten)]
    pub(crate) output: OutputArgs,
}

#[derive(Args, Debug)]
pub(crate) struct JobCancelArgs {
    #[arg(
        value_name = "JOB_ID",
        num_args = 1..,
        required_unless_present = "stdin",
        conflicts_with = "stdin"
    )]
    pub(crate) job_ids: Vec<String>,
    #[arg(long, conflicts_with = "job_ids")]
    pub(crate) stdin: bool,
    #[command(flatten)]
    pub(crate) output: OutputArgs,
}
