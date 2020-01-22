
mod sorted;


pub trait WithSequentialId {
    type Id: Ord + Clone;
    fn id(&self) -> Self::Id;
}
