use std::time::{Instant, Duration};
use std::thread;

pub struct Timeout {
    end_time: Option<Instant>,
    callback: Option<Box<dyn Fn() + Send>>,
    duration: u64,
}

impl Timeout {
    pub fn new(duration: u64, callback: Box<dyn Fn() + Send>) -> Self {
        Timeout {
            end_time: None,
            callback: Some(callback),
            duration,
        }
    }

    pub fn start(&mut self) {
        //let duration = self.duration.clone();
        let duration = Duration::from_millis(self.duration);

        self.end_time = Some(Instant::now() + duration);

        let callback = self.callback.take(); // Take ownership of the callback
        if let Some(cb) = callback {
            // Spawn a new thread to execute the callback after the timeout
            thread::spawn(move || {
                thread::sleep(duration);
                cb();
            });
        }
    }

    pub fn stop(&mut self) {
        self.end_time = None;
    }

    pub fn active(&self) -> bool {
        self.end_time.map_or(false, |end| end > Instant::now())
    }
}