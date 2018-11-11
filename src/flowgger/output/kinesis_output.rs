use super::Output;
use crate::flowgger::config::Config;
use crate::flowgger::merger::Merger;

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

impl  KinesisWorker {
    fn new(
        arx: Arc<Mutex<Receiver<Vec<u8>>>>,
        config: KinesisConfig,
    ) -> Self {
        Self {
            arx,
            config,
            client: KinesisClient::new(config.region.clone()),
            queue: Vec::with_capacity(config.send_max_size),
        }
    }

    fn run(&mut self) {
        //TODO: make chennel
        //TODO: make sender

        loop {
            let b = mdo! {
                lock =<< self.arx.lock();
                bytes =<< lock.recv();
                ret ret(bytes)
            }.unwrap();
            self.queue.push(b);
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