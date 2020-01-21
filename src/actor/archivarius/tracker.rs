use std::cmp::max;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::marker::PhantomData;
use crate::events::EventId;

struct Tracker<I> {
    pending: Vec<I>,
    max_persisted: Option<I>,
}

impl<Id> Tracker<Id>
where
    Id: HasPrevious + Ord + Clone + Debug,
{
    fn new() -> Self {
        Tracker {
            pending: Vec::new(),
            max_persisted: None,
        }
    }

    fn mark_pending(&mut self, id: Id) {
        if let Some(max) = &self.max_persisted {
            if &id < max {
                self.max_persisted = Some(id.previous());
            }
        }
        self.pending.push(id);
    }
    fn mark_persisted(&mut self, id: Id) {
        let index = self.pending.iter().position(|i| *i == id).expect(&format!(
            "Got persisted item `{:?}`, that was never pending. It's a bug",
            &id
        ));
        let item = self.pending.remove(index);
        match &self.max_persisted {
            None => self.max_persisted = Some(item),
            Some(max_persisted) if max_persisted < &item => self.max_persisted = Some(item),
            _ => {}
        }
    }

    fn last_persisted_for_sure(&self) -> Option<Id> {
        if let Some(ref max_persisted) = self.max_persisted {
            return match self.pending.iter().min() {
                None => self.max_persisted.clone(),
                Some(min_pending) => {
                    if max_persisted >= min_pending {
                        None
                    } else {
                        self.max_persisted.clone()
                    }
                }
            };
        }

        None
    }
}

pub trait HasPrevious {
    fn previous(&self) -> Self;
}

impl HasPrevious for EventId {
    fn previous(&self) -> Self {
        unimplemented!()
    }
}


#[cfg(test)]
mod tests {
    use crate::actor::archivarius::tracker::{HasPrevious, Tracker};

    #[test]
    fn has_no_persisted_items_when_just_initialized() {
        let tracker = Tracker::<i32>::new();

        assert_eq!(tracker.last_persisted_for_sure(), None)
    }

    #[test]
    fn has_no_persisted_items_when_there_are_only_pending() {
        let mut tracker = Tracker::<i32>::new();
        tracker.mark_pending(1);

        assert_eq!(tracker.last_persisted_for_sure(), None)
    }

    #[test]
    fn should_give_the_same_item_when_seen_only_one() {
        let mut tracker = Tracker::<i32>::new();
        tracker.mark_pending(1);
        tracker.mark_persisted(1);

        assert_eq!(tracker.last_persisted_for_sure(), Some(1))
    }

    #[test]
    fn should_track_position_when_pending_items_out_of_order() {
        let mut tracker = Tracker::<i32>::new();
        tracker.mark_pending(1);
        tracker.mark_pending(3);
        tracker.mark_pending(2);

        tracker.mark_persisted(1);
        assert_eq!(tracker.last_persisted_for_sure(), Some(1));

        tracker.mark_persisted(2);
        assert_eq!(tracker.last_persisted_for_sure(), Some(2));

        tracker.mark_persisted(3);
        assert_eq!(tracker.last_persisted_for_sure(), Some(3));
    }

    #[test]
    fn should_handle_multiple_items_with_the_same_value() {
        let mut tracker = Tracker::<i32>::new();
        tracker.mark_pending(1);
        tracker.mark_pending(1);

        tracker.mark_persisted(1);
        assert_eq!(tracker.last_persisted_for_sure(), None);

        tracker.mark_persisted(1);
        assert_eq!(tracker.last_persisted_for_sure(), Some(1));
    }

    #[test]
    fn should_handle_persisted_values_out_of_order() {
        let mut tracker = Tracker::<i32>::new();
        tracker.mark_pending(1);
        tracker.mark_pending(2);

        tracker.mark_persisted(2);
        assert_eq!(tracker.last_persisted_for_sure(), None);

        tracker.mark_persisted(1);
        assert_eq!(tracker.last_persisted_for_sure(), Some(2));
    }

    #[test]
    fn when_persisted_is_gt_new_pending() {
        let mut tracker = Tracker::new();
        tracker.mark_pending(2);
        tracker.mark_persisted(2);

        tracker.mark_pending(1);
        assert_eq!(tracker.last_persisted_for_sure(), Some(0));
    }

    impl HasPrevious for i32 {
        fn previous(&self) -> Self {
            self - 1
        }
    }

    // TODO: proptest: take bunch of items, add them as pending, reshuffle the array
    //       take the max element of those and check that max_persisted() equals that element
}
