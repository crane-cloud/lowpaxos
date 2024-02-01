use stateright::actor::Id;
//use std::collections::HashMap;
use stateright::util::HashableHashMap;

#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Profile {
    pub x: u8,
}
impl Profile {
    pub fn new(x: u8) -> Self {
        Profile { x }
    }

    pub fn get_x(&self) -> u8 {
        self.x
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ProfileMatrix {
    data: Vec<Vec<Profile>>,
}

impl ProfileMatrix {
    pub fn new(size: usize) -> Self {
        let data = vec![vec![Profile::new(0); size]; size];
        ProfileMatrix { data }
    }

    pub fn get(&self, row: usize, col: usize) -> Option<&Profile> {
        self.data.get(row).and_then(|r| r.get(col))
    }

    pub fn get_row(&self, row: usize) -> Option<Vec<Profile>> {
        self.data.get(row).cloned()
        //self.data.get(row)
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

#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Serialize)]
pub struct NolerMonitorMatrix {
    pub profile_matrix: HashableHashMap<Id, Profile>,
}

impl NolerMonitorMatrix {
    pub fn new() -> Self {
        NolerMonitorMatrix {
            profile_matrix: HashableHashMap::new(),
        }
    }

    pub fn get(&self, id: &Id) -> Option<&Profile> {
        self.profile_matrix.get(id)
    }

    pub fn set(&mut self, id: Id, profile: Profile) -> Result<(), &'static str> {
        self.profile_matrix.insert(id, profile);
        Ok(())
    }
}