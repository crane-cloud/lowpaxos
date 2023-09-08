use std::sync::{Arc, Mutex, Condvar};
use std::time::Duration;
use std::thread;

pub struct Timeout {
    duration: u64,
    callback: Option<Arc<Mutex<Box<dyn Fn() + Send + Sync>>>>,
    timer_id: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
    execute: Arc<(Mutex<bool>, Condvar)>, // Added Condvar for signaling
}

impl Timeout {
    pub fn new(duration: u64, callback: Box<dyn Fn() + Send + Sync>) -> Timeout {
        Timeout {
            duration,
            callback: Some(Arc::new(Mutex::new(callback))),
            timer_id: Arc::new(Mutex::new(None)),
            execute: Arc::new((Mutex::new(true), Condvar::new())), // Initialize Condvar
        }
    }

    pub fn set_timeout(&mut self, duration: u64) {
        assert!(!self.active());
        self.duration = duration;
    }

    pub fn start(&mut self) -> Result<(), String> {
        if !self.active() {
            if let Some(callback) = self.callback.clone() {
                let timer_id = Arc::clone(&self.timer_id);
                let duration = self.duration;
                let execute = Arc::clone(&self.execute);

                *execute.0.lock().unwrap() = true;

                let handle = thread::spawn(move || {
                    thread::sleep(Duration::from_secs(duration));
                    let (lock, cvar) = &*execute;
                    let mut execute = lock.lock().unwrap();

                    // Wait for a signal to continue or drop the callback
                    while !*execute {
                        execute = cvar.wait(execute).unwrap();
                    }

                    //println!("executing timer callback handle {:?}... at {:?}", thread::current().id(), std::time::Instant::now());
                    let callback = callback.lock().unwrap();
                    (*callback)();
                });

                *timer_id.lock().unwrap() = Some(handle);

                Ok(())
            } else {
                Err("No callback specified".to_string())
            }
        } else {
            Err("Timer already active".to_string())
        }
    }

    pub fn reset(&mut self) -> Result<(), String> {
        if self.active() {
            // Signal the timer thread to drop the callback
            self.stop();

            // Wait for the old timeout thread to exit
            if let Some(handle) = self.timer_id.lock().unwrap().take() {
                handle.join().ok();
            }
        }

        // Reset the timer_id to None
        self.timer_id = Arc::new(Mutex::new(None));

        // Reset the execute flag and start a new timeout
        self.execute = Arc::new((Mutex::new(true), Condvar::new()));
        self.start()
    }

    pub fn active(&self) -> bool {
        self.timer_id.lock().unwrap().is_some()
    }

    pub fn stop(&mut self) {
        if self.active() {
            // Signal the timer thread to drop the callback
            let (lock, cvar) = &*self.execute;
            let mut execute = lock.lock().unwrap();
            *execute = false;
            cvar.notify_all();

            if let Some(handle) = self.timer_id.lock().unwrap().take() {
                //println!("stopping timer handle {:?}... at {:?}\n", handle.thread().id(), std::time::Instant::now());
            }
        }
    }
}