use std::fmt;
use std::str::FromStr;

use rand::{thread_rng, RngCore, SeedableRng};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "kind")]
pub enum RngSource {
    Thread,
    Small(u64),
    Std(u64),
}

impl RngSource {
    pub fn to_rng(&self) -> Box<dyn RngCore> {
        match self {
            RngSource::Thread => Box::new(thread_rng()),
            RngSource::Small(seed) => {
                let rng: rand::rngs::SmallRng =
                    SeedableRng::seed_from_u64(*seed);
                Box::new(rng)
            }
            RngSource::Std(seed) => {
                let rng: rand::rngs::StdRng = SeedableRng::seed_from_u64(*seed);
                Box::new(rng)
            }
        }
    }
}

impl fmt::Display for RngSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RngSource::Thread => write!(f, "thread"),
            RngSource::Small(seed) => write!(f, "small:{}", seed),
            RngSource::Std(seed) => write!(f, "std:{}", seed),
        }
    }
}

impl FromStr for RngSource {
    type Err = eyre::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "thread" {
            Ok(RngSource::Thread)
        } else if s.starts_with("small:") {
            let seed = s.trim_start_matches("small:").parse()?;
            Ok(RngSource::Small(seed))
        } else if s.starts_with("std:") {
            let seed = s.trim_start_matches("std:").parse()?;
            Ok(RngSource::Std(seed))
        } else {
            Err(eyre::eyre!("Invalid RngSource: {}", s))
        }
    }
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::*;

    #[test_case("thread" => RngSource::Thread)]
    #[test_case("std:42" => RngSource::Std(42))]
    #[test_case("small:42" => RngSource::Small(42))]
    fn serialization_round_trip(s: &str) -> RngSource {
        s.parse().unwrap()
    }
}
