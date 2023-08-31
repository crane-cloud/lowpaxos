use std::time::Duration;
use std::thread;

pub struct Timeout {
    duration: u64,
    callback: Box <dyn Fn() + Send + Sync>,
    timer_id: usize,
}

impl Timeout {
    fn new(duration: u64, callback: Box <dyn Fn() + Send + Sync>) -> Timeout {
        Timeout {
            duration: duration,
            callback: callback,
            timer_id: 0,
        }
    }

    fn set_timeout(&mut self, duration: u64) {
        assert!(!self.active());
        self.duration = duration;
    }

    fn start(&mut self) -> u64{
        self.reset()
    }

    fn reset(&mut self) -> u64 {
        self.stop();
        self.timer_id = thread::spawn(move || {
            thread::sleep(Duration::from_millis(self.duration));
            (self.callback)();
        }).id();
        self.timer_id
    }

    fn active(&self) -> bool {
        self.timer_id != 0
    }

    fn stop(&mut self) {
        if self.active() {
            self.timer_id = 0;
        }
    }
}