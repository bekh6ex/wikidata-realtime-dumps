pub mod sorted;

mod join_streams;

pub trait Sequential {
    type Marker: Ord + Clone;
    fn seq_marker(&self) -> Self::Marker;
}
