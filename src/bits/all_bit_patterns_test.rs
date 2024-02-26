#![cfg(test)]

use std::collections::HashMap;

use bitvec::order::{BitOrder, Msb0};
use bitvec::vec::BitVec;
use eyre::{Context, ContextCompat};

use crate::bits::Bits;

const TEST_DATA: &str = include_str!("./all_rotations.txt");

struct TestData {
    code: Bits,

    rotations: HashMap<i32, String>,
}

fn parse_test_data(s: &str) -> eyre::Result<TestData> {
    let lines = s.lines();
    let mut lines = lines.map(|s| s.trim()).filter(|s| !s.is_empty());

    let code: Bits = lines.next().context("Missing code")?.parse()?;

    let mut rotations = HashMap::new();

    for line in lines {
        let mut parts = line.splitn(2, ':');
        let rotation = parts
            .next()
            .context("Missing rotation number")?
            .trim()
            .parse()
            .context("Invalid rotation")?;

        let bit_str = parts.next().context("Missing bit string")?;

        rotations.insert(rotation, bit_str.trim().to_string());
    }

    Ok(TestData { code, rotations })
}

fn bit_vec_to_str<B: BitOrder>(bv: BitVec<u64, B>) -> String {
    let mut s = String::new();

    for (i, bit) in bv.iter().enumerate() {
        if i != 0 && i % 64 == 0 {
            s.push(' ');
        }

        s.push(if *bit { '1' } else { '0' });
    }

    s
}

#[test]
fn all_bit_patterns() -> eyre::Result<()> {
    let test_data = parse_test_data(TEST_DATA).unwrap();

    let rotations: HashMap<i32, String> = test_data
        .code
        .rotations()
        .map(|bits| BitVec::<_, Msb0>::from_slice(bits.0.as_slice()))
        .map(|bv| bit_vec_to_str(bv))
        .enumerate()
        .map(|(i, x)| (i as i32 - 15, x))
        .collect();

    assert_eq!(rotations.len(), test_data.rotations.len());

    let mut keys = rotations.keys().collect::<Vec<_>>();
    keys.sort();

    for k in keys {
        similar_asserts::assert_eq!(
            rotations[k],
            test_data.rotations[k],
            "Testing rotation {}",
            k
        );
    }

    Ok(())
}
