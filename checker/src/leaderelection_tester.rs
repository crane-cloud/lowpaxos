use stateright::semantics::SequentialSpec;
use crate::election_tester::ElectionTester;
use std::collections::{btree_map, BTreeMap, VecDeque};
use std::fmt::Debug;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct LeaderElectionTester<ThreadId, RefObj:SequentialSpec> {
    init_ref_obj: RefObj,
    history_by_thread: BTreeMap<ThreadId, VecDeque<(RefObj::Op, RefObj::Ret)>>,
    in_flight_by_thread: BTreeMap<ThreadId, RefObj::Op>,
    is_valid_history: bool,
}

impl<T: Ord, RefObj:SequentialSpec> LeaderElectionTester<T, RefObj> {
    pub fn new(init_ref_obj: RefObj) -> Self {
        Self {
            init_ref_obj,
            history_by_thread: Default::default(),
            in_flight_by_thread: Default::default(),
            is_valid_history: true,
        }
    }

    pub fn len(&self) -> usize {
        let mut len = self.in_flight_by_thread.len();
        for (_, history) in &self.history_by_thread {
            len += history.len();
        }
        len
    }
}
impl<T, RefObj> ElectionTester<T, RefObj> for LeaderElectionTester<T, RefObj>
    where
    T: Copy + Debug + Ord,
    RefObj: Clone + SequentialSpec,
    RefObj::Op: Clone + Debug,
    RefObj::Ret: Clone + Debug + PartialEq,
    {
        fn on_invoke(&mut self, thread_id: T, op: RefObj::Op) -> Result<&mut Self, String>{
            if !self.is_valid_history {
                return Err("Earlier history was invalid".to_string());
            }

            let in_flight_elem = self.in_flight_by_thread.entry(thread_id); 
            if let btree_map::Entry::Occupied(occupied_op_entry) = in_flight_elem {
                self.is_valid_history = false;
                return Err(format!(
                    "Thread already has an operaton in flight. thread_id = {:?}, op = {:?}, history_by_thread = {:?}",
                    thread_id, occupied_op_entry.get(), self.history_by_thread));
            };
            in_flight_elem.or_insert(op);
            self.history_by_thread
                .entry(thread_id)
                .or_insert_with(VecDeque::new);
            Ok(self)
        }

        fn on_return(&mut self, thread_id: T, ret: RefObj::Ret) -> Result<&mut Self, String> {
            if !self.is_valid_history {
                return Err("Earlier history was invalid".to_string());
            }

            let op = match self.in_flight_by_thread.remove(&thread_id) {
                None => {
                    self.is_valid_history = false;
                    return Err(format!(
                        "There is no in-flight operation for this thread ID. \
                        thread_id = {:?},, unexpected_return = {:?}, history={:?}",
                        thread_id,
                        ret,
                        self.history_by_thread
                            .entry(thread_id)
                            .or_insert_with(VecDeque::new)
                    ));
                }
                Some(op) => op,
            };
            self.history_by_thread
                .entry(thread_id)
                .or_insert_with(VecDeque::new)
                .push_back((op, ret));

            Ok(self)
        }

        fn is_consistent(&self) -> bool {
            self.serialized_history().is_some()
        }
} 

impl<T, RefObj> LeaderElectionTester<T, RefObj>
where
    T: Copy + Debug + Ord,
    RefObj: Clone + SequentialSpec,
    RefObj::Op: Clone + Debug,
    RefObj::Ret: Clone + Debug + PartialEq,
{
    pub fn serialized_history(&self) -> Option<Vec<(RefObj::Op, RefObj::Ret)>>
    where
        RefObj: Clone,
        RefObj::Op: Clone,
        RefObj::Ret: Clone,
    {
        if !self.is_valid_history {
            return None;
        }

        Self::serialize(
            Vec::new(),
            &self.init_ref_obj,
            &self.history_by_thread,
            &self.in_flight_by_thread,
        )
    }

    fn serialize(
        valid_history: Vec<(RefObj::Op, RefObj::Ret)>,
        ref_obj: &RefObj,
        remaining_history_by_thread: &BTreeMap<T, VecDeque<(RefObj::Op, RefObj::Ret)>>, 
        in_flight_by_thread: &BTreeMap<T, RefObj::Op>,
    ) -> Option<Vec<(RefObj::Op, RefObj::Ret)>>
    where
        RefObj: Clone,
        RefObj::Op: Clone,
        RefObj::Ret: Clone,

    {
        let done = remaining_history_by_thread
            .iter()
            .all(|(_id, h)| h.is_empty());
        if done {
            return Some(valid_history);
        }

        for (thread_id, remaining_history) in remaining_history_by_thread.iter(){
            let mut remaining_history_by_thread =
                std::borrow::Cow::Borrowed(remaining_history_by_thread);
            let mut in_flight_by_thread = std::borrow::Cow::Borrowed(in_flight_by_thread);
            let (ref_obj, valid_history) = if remaining_history.is_empty() {
                if !in_flight_by_thread.contains_key(thread_id) {
                    continue;
                }
                let op = in_flight_by_thread.to_mut().remove(thread_id).unwrap();
                let mut ref_obj = ref_obj.clone();
                let ret = ref_obj.invoke(&op);
                let mut valid_history = valid_history.clone();
                valid_history.push((op, ret));
                (ref_obj, valid_history)
            } else {
                let (op, ret) = remaining_history_by_thread
                    .to_mut()
                    .get_mut(thread_id)
                    .unwrap()
                    .pop_front()
                    .unwrap();
                let mut ref_obj = ref_obj.clone();
                if !ref_obj.is_valid_step(&op, &ret) {
                    continue;
                }
                let mut valid_history = valid_history.clone();
                valid_history.push((op, ret));
                (ref_obj, valid_history)
            };

            if let Some(valid_history) = Self::serialize(
                valid_history,
                &ref_obj,
                &remaining_history_by_thread,
                &in_flight_by_thread,
            ) {
                return Some(valid_history);
            }
        }
        None
    }

}

impl<T: Ord, RefObj> Default for LeaderElectionTester<T, RefObj>
where
    RefObj: Default + SequentialSpec,
{
    fn default() -> Self {
        Self::new(RefObj::default())
    }
}

impl<T, RefObj> serde::Serialize for LeaderElectionTester<T, RefObj>
where
    RefObj: serde::Serialize + SequentialSpec,
    RefObj::Op: serde::Serialize,
    RefObj::Ret: serde::Serialize,
    T: serde::Serialize + Ord,
{
    fn serialize<Ser: serde::Serializer>(&self, ser: Ser) -> Result<Ser::Ok, Ser::Error>{
        use serde::ser::SerializeStruct;
        let mut out = ser.serialize_struct("LeaderElectionTester", 4)?;
        out.serialize_field("init_ref_obj", &self.init_ref_obj)?;
        out.serialize_field("history_by_thread", &self.history_by_thread)?;
        out.serialize_field("in_flight_by_thread", &self.in_flight_by_thread)?;
        out.serialize_field("is_valid_history", &self.is_valid_history)?;
        out.end()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use stateright::semantics::register::*;
    use stateright::semantics::vec::*;

    #[test]
    fn rejects_invalid_history() -> Result<(), String> {
        assert_eq!(
            LeaderElectionTester::new(Register('A'))
                .on_invoke(99, RegisterOp::Write('B'))?
                .on_invoke(99, RegisterOp::Write('C')),
            Err("There was already an operation in flight. thread_id = 99, op = Write('B'), history_by_thread = []".to_string()));

        assert_eq!(
            LeaderElectionTester::new(Register('A'))
                .on_invret(99, RegisterOp::Write('B'), RegisterRet::WriteOk)?
                .on_invret(99, RegisterOp::Write('C'), RegisterRet::WriteOk)?
                .on_return(99, RegisterRet::WriteOk),
            Err("There is no in-flight operation for this thread ID. \
                thread_id = 99, \
                unexpected_return = WriteOk, \
                history = [(Write('B'), WriteOk), (Write('C'), WriteOk)]"
                .to_string())
            );

        Ok(())
    }

    #[test]
    fn identifies_serializable_register_history() -> Result<(), String> {
        assert_eq!(
            LeaderElectionTester::new(Register('A'))
                .on_invoke(0, RegisterOp::Write('B'))?
                .on_invret(1, RegisterOp::Read, RegisterRet::ReadOk('A'))?
                .serialized_history(),
            Some(vec![(RegisterOp::Read, RegisterRet::ReadOk('A')),])
        ); 
        
        assert_eq!(
            LeaderElectionTester::new(Register('A'))
                .on_invret(0, RegisterOp::Read, RegisterRet::ReadOk('B'))?
                .on_invoke(1, RegisterOp::Write('B'))?
                .serialized_history(),
            Some(vec![(RegisterOp::Read, RegisterRet::ReadOk('B')),])
        );
        Ok(())
    }

    #[test]
    fn identifies_unserializable_register_history() -> Result<(), String> {
        assert_eq!(
            LeaderElectionTester::new(Vec::new())
                .on_invoke(0, VecOp::Push(10))?
                .serialized_history(),
            Some(vec![])
        );

        assert_eq!(
            LeaderElectionTester::new(Vec::new())
                .on_invoke(0, VecOp::Push(10))?
                .on_invret(1, VecOp::Pop, VecRet::PopOk(None))?
                .serialized_history(),
            Some(
                vec![(VecOp::Pop, VecRet::PopOk(None)),]
            )
        );

        assert_eq!(
            LeaderElectionTester::new(Vec::new())
                .on_invret(1, VecOp::Pop, VecRet::PopOk(Some(10)))?
                .on_invret(0, VecOp::Push(10), VecRet::PushOk)?
                .on_invret(0, VecOp::Pop, VecRet::PopOk(Some(20)))?
                .on_invoke(0, VecOp::Push(30))?
                .on_invret(1, VecOp::Push(20), VecRet::PushOk)?
                .on_invret(1, VecOp::Pop, VecRet::PopOk(None))?
                .serialized_history(),

            Some(vec![
                (VecOp::Push(10), VecRet::PushOk),
                (VecOp::Pop, VecRet::PopOk(Some(10))),
                (VecOp::Push(20), VecRet::PushOk),
                (VecOp::Pop, VecRet::PopOk(Some(20))),
                (VecOp::Pop, VecRet::PopOk(None)),
            ])
        );

        Ok(())
    }

    #[test]
    fn identifies_unserializable_vec_history() -> Result<(), String> {
        assert_eq!(
            LeaderElectionTester::new(Vec::new())
                .on_invret(0, VecOp::Push(10), VecRet::PushOk)?
                .on_invoke(0, VecOp::Push(20))?
                .on_invret(1, VecOp::Len, VecRet::LenOk(2))?
                .on_invret(1, VecOp::Pop, VecRet::PopOk(Some(10)))?
                .on_invret(1, VecOp::Pop, VecRet::PopOk(Some(20)))?
                .serialized_history(),
            None
        );
        Ok(())
    }
}