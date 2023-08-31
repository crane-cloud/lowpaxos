use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::thread;

pub struct Timeout {
   duration: u64,
   callback: Arc<Mutex<Box<dyn Fn() + Send + Sync>>>,
   timer_id: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
}
 
impl Timeout {
   pub fn new(duration: u64, callback: Box<dyn Fn() + Send + Sync>) -> Timeout {
       Timeout {
           duration,
           callback: Arc::new(Mutex::new(callback)),
           timer_id: Arc::new(Mutex::new(None)),
       }
   }
 

   pub fn set_timeout(&mut self, duration: u64) {
       assert!(!self.active());
       self.duration = duration;
   }
 
   pub fn start(&mut self) -> Result<(), String> {
       self.reset()
   }
 
   pub fn reset(&mut self) -> Result<(), String> {
       self.stop();
 
       let callback = Arc::clone(&self.callback);
       let timer_id = Arc::clone(&self.timer_id);
       let duration = self.duration;
 
       let handle = thread::spawn(move || {
           thread::sleep(Duration::from_millis(duration));
           let callback = callback.lock().unwrap();
           (*callback)();
       });
 
       *timer_id.lock().unwrap() = Some(handle);
 
       Ok(())
   }
 
   pub fn active(&self) -> bool {
       self.timer_id.lock().unwrap().is_some()
   }
 
   pub fn stop(&mut self) {
       if self.active() {
           if let Some(handle) = self.timer_id.lock().unwrap().take() {
               handle.join().unwrap();
           }
       }
   }
}