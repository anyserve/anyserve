use std::fmt;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use serde_json::{Map, Value};
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

mod extra_commands;
mod job_commands;
mod serve_auth;
mod serve_config;
mod serve_console;
mod serve_runtime;
#[cfg(test)]
mod test_support;

use extra_commands::{
    DoctorArgs, InvokeArgs, TokenCommands, run_doctor_command, run_invoke_command,
    run_token_command,
};
use job_commands::{JobArgs, run_job_command};
use serve_runtime::{ServeArgs, run as run_serve};

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:50052";
const ENDPOINT_ENV: &str = "ANYSERVE_ENDPOINT";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
enum LogFormat {
    #[default]
    Text,
    Json,
}

#[derive(Parser, Debug)]
#[command(
    name = "anyserve",
    version,
    about = "Backend-independent execution and scheduling with outbound workers."
)]
struct Cli {
    #[arg(short, long, global = true)]
    verbose: bool,
    #[arg(long, default_value = "info", global = true)]
    log_level: String,
    #[arg(long, value_enum, default_value = "text", global = true)]
    log_format: LogFormat,
    #[arg(long, global = true)]
    endpoint: Option<String>,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Serve(ServeArgs),
    Job(JobArgs),
    Token {
        #[command(subcommand)]
        command: TokenCommands,
    },
    Invoke(InvokeArgs),
    Doctor(DoctorArgs),
}

/// The trimmed flag value, else the trimmed `var` from `env`; blank values count as unset.
pub(crate) fn arg_or_env(
    arg: Option<String>,
    var: &str,
    env: impl FnOnce(&str) -> Option<String>,
) -> Option<String> {
    serve_config::nonempty_trimmed(arg).or_else(|| serve_config::nonempty_trimmed(env(var)))
}

/// Reads a process environment variable; the default `env` for [`arg_or_env`].
pub(crate) fn process_env(var: &str) -> Option<String> {
    std::env::var(var).ok()
}

fn resolve_endpoint(
    cli_endpoint: Option<String>,
    env: impl FnOnce(&str) -> Option<String>,
) -> String {
    arg_or_env(cli_endpoint, ENDPOINT_ENV, env).unwrap_or_else(|| DEFAULT_ENDPOINT.to_owned())
}

#[tokio::main]
async fn main() -> Result<()> {
    let Cli {
        verbose,
        log_level,
        log_format,
        endpoint,
        command,
    } = Cli::parse();
    let is_serve = matches!(command, Commands::Serve(_));
    init_tracing(verbose, &log_level, log_format, is_serve)?;
    let endpoint = resolve_endpoint(endpoint, process_env);

    match command {
        Commands::Serve(args) => run_serve(args, log_format == LogFormat::Json).await,
        Commands::Job(args) => run_job_command(&endpoint, args).await,
        Commands::Token { command } => run_token_command(command),
        Commands::Invoke(args) => run_invoke_command(&endpoint, args).await,
        Commands::Doctor(args) => run_doctor_command(&endpoint, args).await,
    }
}

#[derive(Default)]
struct JsonFieldVisitor {
    fields: Map<String, Value>,
}

impl Visit for JsonFieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields
            .insert(field.name().to_owned(), Value::String(value.to_owned()));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .insert(field.name().to_owned(), Value::Bool(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .insert(field.name().to_owned(), Value::Number(value.into()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_owned(), Value::Number(value.into()));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        if let Some(n) = serde_json::Number::from_f64(value) {
            self.fields
                .insert(field.name().to_owned(), Value::Number(n));
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.fields
            .insert(field.name().to_owned(), Value::String(format!("{value:?}")));
    }
}

struct JsonEventFormatter;

impl<S, N> FormatEvent<S, N> for JsonEventFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();
        let mut visitor = JsonFieldVisitor::default();
        event.record(&mut visitor);

        let mut root = Map::new();
        root.insert(
            "timestamp".to_owned(),
            Value::Number(anyserve_core::now_ms().into()),
        );
        root.insert(
            "level".to_owned(),
            Value::String(meta.level().as_str().to_owned()),
        );
        root.insert("target".to_owned(), Value::String(meta.target().to_owned()));
        // serde_json's `Map` is sorted, so keys print alphabetically; fields override root keys.
        root.extend(visitor.fields);
        let encoded = serde_json::to_string(&Value::Object(root)).map_err(|_| fmt::Error)?;
        writeln!(writer, "{encoded}")
    }
}

fn try_init_writer<W>(filter: EnvFilter, log_format: LogFormat, writer: W) -> Result<()>
where
    W: for<'a> tracing_subscriber::fmt::MakeWriter<'a> + Send + Sync + 'static,
{
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(writer);
    let initialized = match log_format {
        LogFormat::Text => builder.with_target(false).compact().try_init(),
        LogFormat::Json => builder.event_format(JsonEventFormatter).try_init(),
    };
    initialized.map_err(|error| anyhow::anyhow!("initialize tracing: {error}"))
}

fn init_tracing(
    verbose: bool,
    log_level: &str,
    log_format: LogFormat,
    to_stdout: bool,
) -> Result<()> {
    let level = if verbose { "debug" } else { log_level };
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(level))
        .context("build log filter")?;

    if to_stdout {
        try_init_writer(filter, log_format, std::io::stdout)
    } else {
        try_init_writer(filter, log_format, std::io::stderr)
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_ENDPOINT, ENDPOINT_ENV, JsonEventFormatter, resolve_endpoint};

    #[test]
    fn endpoint_resolution_prefers_cli_then_env_then_default() {
        assert_eq!(
            resolve_endpoint(Some("http://cli:1234".into()), |_| Some(
                "http://env:9".into()
            )),
            "http://cli:1234"
        );
        assert_eq!(
            resolve_endpoint(None, |k| (k == ENDPOINT_ENV)
                .then(|| "https://env:443".into())),
            "https://env:443"
        );
        assert_eq!(
            resolve_endpoint(None, |_| Some("   ".into())),
            DEFAULT_ENDPOINT
        );
    }

    #[test]
    fn json_event_formatter_emits_single_line_valid_json() {
        use std::io::{self, Write};
        use std::sync::{Arc, Mutex};
        use tracing::subscriber::with_default;

        #[derive(Clone, Default)]
        struct BufWriter(Arc<Mutex<Vec<u8>>>);
        impl Write for BufWriter {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufWriter {
            type Writer = BufWriter;
            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        let buf = BufWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .event_format(JsonEventFormatter)
            .finish();
        with_default(subscriber, || {
            tracing::info!(job_id = "j-1", count = 3u64, "job processed");
        });

        let line = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        assert_eq!(line.lines().count(), 1, "{line}");
        let parsed: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert!(parsed["timestamp"].as_u64().is_some());
        assert_eq!(parsed["level"], "INFO");
        assert_eq!(parsed["message"], "job processed");
        assert_eq!(parsed["job_id"], "j-1");
        assert_eq!(parsed["count"], 3);
    }

    #[test]
    fn job_cli_accepts_token_and_namespace_before_or_after_subcommand() {
        use super::{Cli, Commands};
        use clap::Parser;

        for argv in [
            vec![
                "anyserve",
                "job",
                "--token",
                "tok-1",
                "--namespace",
                "ns-a",
                "ls",
            ],
            vec![
                "anyserve",
                "job",
                "ls",
                "--token",
                "tok-1",
                "--namespace",
                "ns-a",
            ],
        ] {
            let parsed = Cli::try_parse_from(argv).unwrap();
            let Commands::Job(job) = parsed.command else {
                panic!("expected Job command");
            };
            assert_eq!(job.token.as_deref(), Some("tok-1"));
            assert_eq!(job.namespace.as_deref(), Some("ns-a"));
        }
    }
}
