#![feature(array_chunks)]
#![allow(clippy::type_complexity, clippy::comparison_chain)]

pub mod arch;
pub mod bits;
pub mod config;
pub mod coordinator;
pub mod db;
mod debug_encoding;
pub mod distance;
pub mod encoded_bits;
pub mod health_check;
pub mod iris;
pub mod participant;
pub mod slice_utils;
pub mod template;
pub mod utils;
