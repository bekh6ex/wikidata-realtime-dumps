use std::fmt::Debug;

pub mod join_streams;

pub trait Sequential {
    type Marker: Ord + Clone + Debug;
    fn seq_marker(&self) -> Self::Marker;
}
