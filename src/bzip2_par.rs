use std::pin::Pin;

use futures::future::Ready;
use futures::stream::{Fuse, FusedStream, Peekable};
use futures::task::{Context, Poll};
use futures::*;
use log::*;

use pin_utils::{unsafe_pinned, unsafe_unpinned};

use crate::prelude::*;
use futures::future::Either;
use std::cmp::Ordering;
use bzip2_rs::decoder::{DecoderError, ParallelDecoder, ReadState};
use bzip2_rs::RayonThreadPool;


pub struct Bzip2Par<S> {
    inner: S,
    decoder: ParallelDecoder<RayonThreadPool>,
}

impl<S> Unpin for Bzip2Par<S>
{}

impl<B, S> Bzip2Par<Fuse<S>>
    where
        B: AsRef<[u8]>,
        S: Stream<Item=std::io::Result<B>>
{
    unsafe_pinned!(inner: Stream<Item=std::io::Result<B>>);

    pub fn new(inner: S) -> Self {
        let decoder: ParallelDecoder<RayonThreadPool> = bzip2_rs::decoder::ParallelDecoder::new(bzip2_rs::RayonThreadPool, 200 * 1024 * 1024);


        Bzip2Par { inner: inner.fuse(), decoder }
    }
}

impl<B, S> Stream for Bzip2Par<Fuse<S>>
    where
        B: AsRef<[u8]>,
        S: Stream<Item=std::io::Result<B>>
{
    type Item = std::io::Result<bytes::Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // {
        //     let upstream_data: Poll<Option<_>> = self
        //         .as_mut()
        //         .inner().poll_next(cx);
        //
        //     match ready!(upstream_data) {
        //         Some(Ok(data)) => {
        //             if (data.as_ref().len() != 0)  {
        //                 // To avoid decoder thinking it is the end of the file
        //                 let result = self.decoder.write(data.as_ref());
        //                 match result {
        //                     Ok(_) => {
        //                         //do nothing
        //                     }
        //                     Err(e) => {
        //                         return Poll::Ready(Some(Err(e.into())))
        //                     }
        //                 }
        //             }
        //         }
        //         None => {
        //             // This is the end of the file
        //             self.decoder.write(&[]);
        //         }
        //         Some(Err(e)) => {
        //             return Poll::Ready(Some(Err(e.into())))
        //         }
        //     }
        // }

        let mut buf = [0; 8192*10];

        match self.as_mut().decoder.read(&mut buf) {
            Ok(ReadState::NeedsWrite) => {
                // `ParallelDecoder` needs more data to be written to it before it
                // can decode the next block.
                // If we reached the end of the file `compressed_file.len()` will be 0,
                // signaling to the `Decoder` that the last block is smaller and it can
                // proceed with reading.
                // self.decoder.write(&compressed_file);
                // compressed_file = &[];

                loop {
                    let upstream_data: Poll<Option<_>> = self
                        .as_mut()
                        .inner().poll_next(cx);

                    match ready!(upstream_data) {
                        Some(Ok(data)) => {
                            if (data.as_ref().len() != 0)  {
                                // To avoid decoder thinking it is the end of the file
                                let result = self.decoder.write(data.as_ref());
                                match result {
                                    Ok(_) => {
                                        continue
                                    }
                                    Err(e) => {
                                        return Poll::Ready(Some(Err(e.into())))
                                    }
                                }
                            }
                        }
                        #[allow(unused_must_use)] // The only error is that we reached EOF - ignore
                        None => {
                            // This is the end of the file
                            self.decoder.write(&[]);
                        }
                        Some(Err(e)) => {
                            return Poll::Ready(Some(Err(e.into())))
                        }
                    }
                }
            }
            Ok(ReadState::Read(n)) => {
                // `n` uncompressed bytes have been read into `buf`
                // output.extend_from_slice(&buf[..n]);
                let slice = &buf[..n];
                let bytes1 = bytes::Bytes::copy_from_slice(slice);
                return Poll::Ready(Some(Ok(bytes1)));
            }
            Ok(ReadState::Eof) => {
                // we reached the end of the file
                return Poll::Ready(None);
            }
            Err(e) => {
                return Poll::Ready(Some(Err(e.into())));
            }
        }

        }

}
