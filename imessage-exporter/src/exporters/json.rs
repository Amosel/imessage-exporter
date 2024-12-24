use crate::app::runtime::Config;
use crate::app::error::RuntimeError;
use serde_json::json;
use imessage_database::tables::messages::Message;
use rusqlite::Error as RusqliteError;
use imessage_database::error::table::TableError;
use crate::Exporter;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::collections::HashMap;
use crate::app::progress::build_progress_bar_export;
use imessage_database::tables::table::Table;
use imessage_database::util::dates::{format, get_local_time};

impl From<RusqliteError> for RuntimeError {
    fn from(err: RusqliteError) -> RuntimeError {
        RuntimeError::DatabaseError(TableError::Messages(err))
    }
}

impl From<TableError> for RuntimeError {
    fn from(err: TableError) -> RuntimeError {
        RuntimeError::DatabaseError(err)
    }
}

impl From<std::io::Error> for RuntimeError {
    fn from(err: std::io::Error) -> RuntimeError {
        RuntimeError::DiskError(err)
    }
}

pub struct JSONExporter<'a> {
    /// Data that is setup from the application's runtime
    pub config: &'a Config,
    /// Handles to files we want to write messages to
    /// Map of resolved chatroom file location to a buffered writer
    pub files: HashMap<String, BufWriter<File>>,
    /// Writer instance for orphaned messages
    pub orphaned: BufWriter<File>,
}

impl<'a> JSONExporter<'a> {
    fn format_custom(&self, message: &Message) -> serde_json::Value {
        let sender = self.config.who(
            message.handle_id,
            message.is_from_me,
            &message.destination_caller_id,
        ).to_string();

        let receiver = if message.is_from_me {
            self.config.who(
                None,
                false,
                &message.destination_caller_id,
            ).to_string()
        } else {
            self.config.who(
                message.handle_id,
                true,
                &message.destination_caller_id,
            ).to_string()
        };

        let format_timestamp = |ts: i64| {
            if ts == 0 {
                "N/A".to_string()
            } else {
                format(&get_local_time(&ts, &self.config.offset))
            }
        };

        json!({
            "timestamp": format_timestamp(message.date),
            "sender": sender,
            "receiver": receiver,
            "message": message.text,
            "conversation_id": message.chat_id.unwrap_or_default().to_string(),
            "guid": message.guid,
            "service": message.service,
            "is_read": message.is_read,
            "date_read": format_timestamp(message.date_read),
            "date_delivered": format_timestamp(message.date_delivered),
            "deleted": message.is_deleted(),
        })
    }
}

impl<'a> Exporter<'a> for JSONExporter<'a> {
    fn new(config: &'a Config) -> Result<Self, RuntimeError> {
        let mut orphaned = config.options.export_path.clone();
        orphaned.push("orphaned");
        orphaned.set_extension("json");
        let file = File::options()
            .append(true)
            .create(true)
            .open(&orphaned)
            .map_err(|err| RuntimeError::CreateError(err, orphaned.clone()))?;

        Ok(JSONExporter {
            config,
            files: HashMap::new(),
            orphaned: BufWriter::new(file),
        })
    }

    fn iter_messages(&mut self) -> Result<(), RuntimeError> {
        eprintln!(
            "Exporting to {} as json...",
            self.config.options.export_path.display()
        );

        let total_messages = Message::get_count(&self.config.db, &self.config.options.query_context)?;
        let pb = build_progress_bar_export(total_messages);

        let mut statement = Message::stream_rows(&self.config.db, &self.config.options.query_context)?;

        let messages = statement.query_map([], |row| Ok(Message::from_row(row)))?;

        let mut conversation_map: HashMap<Option<i32>, Vec<serde_json::Value>> = HashMap::new();

        for message in messages {
            let mut msg = Message::extract(message)?;

            let _ = msg.generate_text(&self.config.db);

            let json_message = self.format_custom(&msg);

            conversation_map.entry(msg.chat_id).or_default().push(json_message);

            pb.inc(1);
        }
        pb.finish();

        for (chat_id, messages_array) in conversation_map {
            let writer = self.get_or_create_file(&Message { chat_id, ..Default::default() })?;
            writeln!(writer, "{}", serde_json::to_string(&messages_array).unwrap())?;
        }

        Ok(())
    }

    fn get_or_create_file(&mut self, message: &Message) -> Result<&mut BufWriter<File>, RuntimeError> {
        match self.config.conversation(message) {
            Some((chatroom, _)) => {
                let filename = self.config.filename(chatroom);
                let path = self.config.options.export_path.join(filename).with_extension("json");
                if !self.files.contains_key(&path.to_string_lossy().to_string()) {
                    let file = File::options()
                        .append(true)
                        .create(true)
                        .open(&path)
                        .map_err(|err| RuntimeError::CreateError(err, path.clone()))?;
                    let writer = BufWriter::new(file);
                    self.files.insert(path.to_string_lossy().to_string(), writer);
                }
                Ok(self.files.get_mut(&path.to_string_lossy().to_string()).unwrap())
            }
            None => Ok(&mut self.orphaned),
        }
    }
} 