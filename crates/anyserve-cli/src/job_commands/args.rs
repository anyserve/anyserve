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
    #[value(alias = "pending")]
    Queued,
    #[value(alias = "leased")]
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Args, Debug)]
pub(crate) struct JobLsArgs {
    #[arg(long = "state", value_enum)]
    pub(crate) states: Vec<JobStateFilter>,
    #[arg(long, default_value_t = 100)]
    pub(crate) limit: u32,
    #[arg(long)]
    pub(crate) page_token: Option<String>,
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

impl JobStateFilter {
    pub(crate) fn protocol(self) -> i32 {
        use anyserve_client::execution::v2::JobState;
        (match self {
            Self::Queued => JobState::Queued,
            Self::Running => JobState::Running,
            Self::Succeeded => JobState::Succeeded,
            Self::Failed => JobState::Failed,
            Self::Cancelled => JobState::Cancelled,
        }) as i32
    }
}
