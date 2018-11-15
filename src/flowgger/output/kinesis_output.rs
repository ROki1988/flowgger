use super::Output;
use crate::flowgger::config::Config;
use crate::flowgger::merger::Merger;

use std::fmt;
use std::fmt::Display;
use failure::{Backtrace, Context, Fail};
use mdo::result::{bind, ret};

use std::io::{stderr, Write};
use std::process::exit;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use rusoto_core::{
    Region,
};
use rusoto_kinesis::{
    KinesisClient,
    PutRecordsRequestEntry
};

pub struct KinesisOutput {
    config: KinesisConfig,
    threads: u32,
}

#[derive(Clone)]
struct KinesisConfig{
    region: Region,
    stream_name: String,
    send_max_size: usize,
}

pub struct KinesisWorker {
    arx: Arc<Mutex<Receiver<Vec<u8>>>>,
    config: KinesisConfig,
    client: KinesisClient,
    queue: Vec<Vec<u8>>,
}

#[derive(Fail, Debug)]
pub enum ErrorKind {
    #[fail(display = "IO error")]
    Io,
}

#[derive(Debug)]
pub struct Error {
    inner: Context<ErrorKind>,
}

impl Fail for Error {
    fn cause(&self) -> Option<&Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl Error {
    pub fn new(inner: Context<ErrorKind>) -> Error {
        Error { inner }
    }

    pub fn kind(&self) -> &ErrorKind {
        self.inner.get_context()
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Error {
        Error {
            inner: Context::new(kind),
        }
    }
}

impl From<Context<ErrorKind>> for Error {
    fn from(inner: Context<ErrorKind>) -> Error {
        Error { inner }
    }
}

impl From<std::sync::mpsc::RecvError> for Error {
    fn from(error: std::sync::mpsc::RecvError) -> Error {
        Error {
            inner: error.context(ErrorKind::Io),
        }
    }
}

impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(error: std::sync::PoisonError<T>) -> Error {
        Error {
            // TODO: fix when resolve issue
            // https://github.com/rust-lang-nursery/failure/issues/192
            inner: Context::new(ErrorKind::Io),
        }
    }
}

impl  KinesisWorker {
    fn new(
        arx: Arc<Mutex<Receiver<Vec<u8>>>>,
        config: KinesisConfig,
    ) -> Self {
        Self {
            queue: Vec::with_capacity(config.send_max_size),
            client: KinesisClient::new(config.region.clone()),
            arx,
            config,
        }
    }

    fn run(&mut self) {
        //TODO: make chennel
        //TODO: make sender

        loop {
            let r: Result<_, Error> = self.arx.lock().map_err(Into::into)
                .and_then(|lock| lock.recv().map_err(Into::into))
                .map(|b| self.queue.push(b));
        }
    }
}

impl Output for KinesisOutput {
    fn start(&self, arx: Arc<Mutex<Receiver<Vec<u8>>>>, merger: Option<Box<Merger>>) {
        if merger.is_some() {
            let _ = writeln!(stderr(), "Output framing is ignored with the Kinesis output");
        }

        let arx = Arc::clone(&arx);
        let config = self.config.clone();
        thread::spawn(move || {
            let mut worker = KinesisWorker::new(arx, config);
            worker.run();        
        });
    }
}