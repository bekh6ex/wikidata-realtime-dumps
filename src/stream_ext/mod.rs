pub mod sorted;

pub trait Sequential {
    type Marker: Ord + Clone;
    fn seq_marker(&self) -> Self::Marker;
}
