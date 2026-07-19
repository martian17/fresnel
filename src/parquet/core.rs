use std::sync::mpsc::{sync_channel, channel, Receiver, SyncSender, Sender, SendError, RecvTimeoutError};
use std::cmp::Ordering;
use std::sync::{Mutex, Condvar, Arc};
use std::thread;
use std::collections::BinaryHeap;
use std::sync::mpsc;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Result};
use arrow::array::{UInt16Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use ::parquet::arrow::ArrowWriter;

use crate::types::core::{
    Time,
    NormalizedTimeTag,
};

pub struct TimeTagBatch {
    pub start_time: Time,
    pub end_time: Time,
    pub batch: Vec<Time>,
}

enum ParquetWorkerEvent {
    AddChannel{
        channel_id: u16,
        receiver: Receiver<TimeTagBatch>,
    },
    // We may not have to remove anything at all in the future
    // RemoveChannel(u16),

    Start,
    // Stop(Time)
}

struct ChannelBuffer {
    channel_id: u16,
    receiver: Receiver<TimeTagBatch>,
    // tags received but not yet safe to emit (may still be preceded by
    // tags from a channel whose frontier lags behind)
    pending: Vec<Time>,
    // every future tag on this channel is >= frontier: batches arrive in
    // leading-edge order and a tag never precedes its packet's leading edge
    frontier: Time,
}

impl ChannelBuffer {
    fn absorb(&mut self, batch: TimeTagBatch) {
        self.pending.extend(batch.batch);
        self.frontier = batch.end_time;
    }
}

pub struct ParquetWorker {
    rx_channels: Vec<ChannelBuffer>,
    ctrl_channel: Receiver<ParquetWorkerEvent>,
    writer_channel: SyncSender<Vec<NormalizedTimeTag>>,
}


impl ParquetWorker {
    pub fn spawn(output_dir: PathBuf, name: String) -> ParquetHandle {
        let (ctrl_tx, ctrl_rx) = mpsc::channel::<ParquetWorkerEvent>();
        // bounded so a dying disk backpressures the sim instead of eating RAM
        let (writer_tx, writer_rx) = sync_channel::<Vec<NormalizedTimeTag>>(64);
        let writer_thread_handle = thread::spawn(move || {
            TimeTagStreamParquetWriter::new().write(writer_rx, &output_dir, &name).unwrap();
        });
        let mut worker = Self{
            rx_channels: Vec::new(),
            ctrl_channel: ctrl_rx,
            writer_channel: writer_tx,
        };
        ParquetHandle{
            ctrl_channel: ctrl_tx,
            thread_handle: thread::spawn(move ||{
                worker.run();
            }),
            writer_thread_handle,
        }
    }
    // the channel gating the safe emission point: the one whose frontier is oldest
    fn youngest_channel(&self) -> usize {
        let mut index = 0;
        for i in 1..self.rx_channels.len() {
            if self.rx_channels[i].frontier < self.rx_channels[index].frontier {
                index = i;
            }
        }
        index
    }
    fn run(&mut self) {
        loop {
            match self.ctrl_channel.recv().unwrap() {
                ParquetWorkerEvent::AddChannel{channel_id, receiver} => {
                    self.rx_channels.push(ChannelBuffer{
                        channel_id,
                        receiver,
                        pending: Vec::new(),
                        frontier: 0,
                    });
                },
                ParquetWorkerEvent::Start => {
                    break;
                }
            }
        }
        assert!(!self.rx_channels.is_empty(), "ParquetWorker started with no channels");
        loop {
            // wait on the laggard, since only its progress can advance the safe
            // point. The timeout matters: blocking here indefinitely while the
            // other taggers fill up would backpressure their claimers (and in
            // turn the whole sim) into a standstill, so we periodically fall
            // through to the drain below no matter what
            let laggard = self.youngest_channel();
            match self.rx_channels[laggard].receiver.recv_timeout(Duration::from_millis(1)) {
                Ok(batch) => self.rx_channels[laggard].absorb(batch),
                Err(RecvTimeoutError::Timeout) => {},
                Err(RecvTimeoutError::Disconnected) => {
                    panic!("ParquetWorker: time tag channel hung up (graceful stop is not implemented yet)");
                },
            }
            // opportunistically drain every channel so no claimer ever blocks
            // on a full tagger queue behind our back
            for channel in self.rx_channels.iter_mut() {
                while let Ok(batch) = channel.receiver.try_recv() {
                    channel.absorb(batch);
                }
            }
            // everything at or before the slowest frontier is final: no channel
            // can produce an older tag anymore
            let safe = self.rx_channels.iter().map(|c| c.frontier).min().unwrap();
            let mut out: Vec<NormalizedTimeTag> = Vec::new();
            for channel in self.rx_channels.iter_mut() {
                let (emit, keep): (Vec<Time>, Vec<Time>) =
                    channel.pending.drain(..).partition(|t| *t <= safe);
                channel.pending = keep;
                out.extend(emit.into_iter().map(|t| NormalizedTimeTag{
                    channel_id: channel.channel_id,
                    time_tag_ps: t,
                }));
            }
            if !out.is_empty() {
                out.sort_by_key(|tag| tag.time_tag_ps);
                self.writer_channel.send(out).unwrap();
            }
        }
    }
}


// let parquet_writer = ParquetWorker::spawn()
// // and do something like
// spd1.connect_time_tagger(parquet_writer.add_channel(spd1.id))


pub struct ParquetHandle {
    ctrl_channel: Sender<ParquetWorkerEvent>,
    thread_handle: thread::JoinHandle<()>,
    writer_thread_handle: thread::JoinHandle<()>,
}

impl ParquetHandle {
    pub fn add_channel(&self, channel_id: u16) -> SyncSender<TimeTagBatch> {
        // shallow on purpose: the merge loop drains these continuously, and a
        // deep buffer here would only hide a broken merge loop
        let (tx, rx) = sync_channel::<TimeTagBatch>(16);
        self.ctrl_channel.send(ParquetWorkerEvent::AddChannel{
            channel_id,
            receiver: rx,
        }).unwrap();
        tx
    }
    pub fn start(&self) {
        self.ctrl_channel.send(ParquetWorkerEvent::Start).unwrap();
    }
    pub fn join(self) {
        self.thread_handle.join().unwrap();
        self.writer_thread_handle.join().unwrap();
    }
}


// this is from another project. I want this to follow the same format

pub struct TimeTagStreamParquetWriter {
    // The maximum number of total rows (records) that should be
    // collected before writing to disk.
    max_chunk_rows: usize,
    // The maximum number of total rows (records) that should be
    // allowed per file.
    max_file_rows: usize,
}

impl TimeTagStreamParquetWriter {
    #[must_use]
    pub fn new() -> TimeTagStreamParquetWriter {
        TimeTagStreamParquetWriter {
            max_chunk_rows: 20_000_000,
            max_file_rows: 200_000_000,
        }
    }

    pub fn write(
        &self,
        rx_channel: mpsc::Receiver<Vec<NormalizedTimeTag>>,
        output_dir: &Path,
        name: &str,
    ) -> Result<()> {
        if !output_dir.is_dir() {
            bail!(
                "Requested output path {} is not a directory.",
                output_dir.display()
            );
        }
        let fields = vec![
            Field::new("channel", DataType::UInt16, false),
            Field::new("time_tag", DataType::UInt64, false),
        ];
        let schema: Arc<Schema> = Schema::new(fields).into();

        let max_chunk_count = self.max_file_rows / self.max_chunk_rows;
        let file_timestamp = Utc::now().format("%Y%m%dT%H%M%SZ");

        let mut total_files = 1;
        let initial_file = File::create_new(
            output_dir.join(format!("{file_timestamp}_{name}_{total_files:0>4}.parquet")),
        )?;
        let mut arrow_writer = ArrowWriter::try_new(initial_file, schema.clone(), None)?;
        let mut channel_array_builder = UInt16Array::builder(self.max_chunk_rows);
        let mut time_tag_array_builder = UInt64Array::builder(self.max_chunk_rows);
        let mut array_length = 0;
        let mut chunk_count = 0;
        for rx_batch in rx_channel {
            for event in rx_batch {
                array_length += 1;
                channel_array_builder.append_value(event.channel_id);
                time_tag_array_builder.append_value(event.time_tag_ps);
            }

            if array_length >= self.max_chunk_rows {
                // write current batch into current file
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(channel_array_builder.finish()),
                        Arc::new(time_tag_array_builder.finish()),
                    ],
                )?;
                arrow_writer.write(&batch)?;
                array_length = 0;
                chunk_count += 1;
            }

            if chunk_count > max_chunk_count {
                // close and replace file
                arrow_writer.close()?;
                chunk_count = 0;
                total_files += 1;

                let new_file = File::create_new(
                    output_dir.join(format!("{file_timestamp}_{name}_{total_files:0>4}.parquet")),
                )?;
                arrow_writer = ArrowWriter::try_new(new_file, schema.clone(), None)?;
            }
        }

        // write any remaining data
        if array_length > 0 {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(channel_array_builder.finish()),
                    Arc::new(time_tag_array_builder.finish()),
                ],
            )?;
            arrow_writer.write(&batch)?;
        }
        arrow_writer.close()?;

        Ok(())
    }
}

impl Default for TimeTagStreamParquetWriter {
    fn default() -> Self {
        Self::new()
    }
}
