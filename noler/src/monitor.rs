#[derive(Clone)]
pub struct Profile {
    pub x: f32,
}

impl Profile {
    pub fn new(x: f32) -> Self {
        Profile { x }
    }

    pub fn get_x(&self) -> f32 {
        self.x
    }
}

#[derive(Clone)]
pub struct ProfileMatrix {
    data: Vec<Vec<Profile>>,
}

impl ProfileMatrix {
    pub fn new(size: usize) -> Self {
        let data = vec![vec![Profile::new(0.0); size]; size];
        ProfileMatrix { data }
    }

    pub fn get(&self, row: usize, col: usize) -> Option<&Profile> {
        self.data.get(row).and_then(|r| r.get(col))
    }

    pub fn set(&mut self, row: usize, col: usize, profile: Profile) -> Result<(), &'static str> {
        if row < self.data.len() && col < self.data[row].len() {
            self.data[row][col] = profile;
            Ok(())
        } else {
            Err("Index out of bounds")
        }
    }
}




// use rand::Rng;

// #[derive(Clone)]
// pub struct Profile {
//     x: f32,
//     y: f32,
// }

// impl Profile {
//     fn get_x(&self) -> f32 {
//         self.x
//     }

//     fn get_y(&self) -> f32 {
//         self.y
//     }
// }
// #[derive(Clone)]
// pub struct ProfileMatrix {
//     data: Vec<Profile>,
// }

// impl ProfileMatrix {
//     pub fn new(replicas: usize) -> Self {
//         let mut data = Vec::new();
//         for _ in 0..replicas {
//             data.push(Profile { 
//                 x: rand::thread_rng().gen_range(0.0..1.0),
//                 y: rand::thread_rng().gen_range(0.0..1.0),
//             });
//         }
//         ProfileMatrix { data }
//     }

//     fn set_profile(&mut self, col: usize, profile: Profile) {
//         if let Some(target_profile) = self.data.get_mut(col) {
//             *target_profile = profile;
//         }
//     }

//     pub fn get_full_profile(&self, col: usize) -> Option<&Profile> {
//         self.data.get(col)
//     }

//     pub fn get_profile_x(&self, col: usize) -> Option<f32> {
//         if let Some(profile) = self.data.get(col) {
//             Some(profile.get_x())
//         } else {
//             None
//         }
//     }

//     pub fn get_profile_y(&self, col: usize) -> Option<f32> {
//         if let Some(profile) = self.data.get(col) {
//             Some(profile.get_y())
//         } else {
//             None
//         }
//     }
// }

