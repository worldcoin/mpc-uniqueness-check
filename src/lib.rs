#![feature(array_chunks)]
#![allow(clippy::type_complexity, clippy::comparison_chain)]

pub mod arch;
pub mod bits;
pub mod config;
pub mod coordinator;
pub mod db;
pub mod distance;
pub mod encoded_bits;
pub mod health_check;
pub mod iris_db;
pub mod participant;
pub mod rng_source;
pub mod template;
pub mod utils;
pub mod snapshot;
pub mod error;
