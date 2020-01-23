use std::fmt::Debug;

pub mod sorted;

pub mod continuous_download;
pub mod join_streams;

pub trait Sequential {
    type Marker: Ord + Clone + Debug;
    fn seq_marker(&self) -> Self::Marker;
}
