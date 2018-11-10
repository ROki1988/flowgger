use super::Output;
use crate::flowgger::config::Config;
use crate::flowgger::merger::Merger;

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
    merger: Option<Box<Merger + Send>>,
    config: KinesisConfig,
    client: KinesisClient,
    queue: Vec<PutRecordsRequestEntry>,
}

impl  KinesisWorker {
    fn new(
        arx: Arc<Mutex<Receiver<Vec<u8>>>>,
        merger: Option<Box<Merger + Send>>,
        config: KinesisConfig,
    ) -> Self {
        Self {
            arx,
            merger,
            config,
            client: KinesisClient::new(config.region.clone()),
            queue: Vec::with_capacity(config.send_max_size),
        }
    }
}