use std::time::Duration;
use std::thread;

pub struct Timeout<F>
where
    F: FnMut() + Send,
{
    duration: Duration,
    callback: F,
}

impl<F> Timeout<F>
where
    F: FnMut() + Send,
{
    pub fn new(duration: Duration, callback: F) -> Self {
        Timeout { duration, callback }
    }

    // pub fn start(&mut self) {
    //     let callback = &mut self.callback;
    //     let _ = thread::spawn(move || {
    //         thread::sleep(self.duration);
    //         callback();
    //     });
    // }
}