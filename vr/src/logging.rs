use log::{Level, Metadata, Record};

pub struct VrLogger;

impl log::Log for VrLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Info
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!(
                "[{}] {} - {}",
                0, // Replace with your index
                record.level(),
                record.args()
            );
        }
    }

    fn flush(&self) {}
}