// Reference: https://github.com/worldcoin/iris-matching/blob/main/hamming/hamming.go#L261
pub fn unpack_iris_code64(c: &[u64]) -> Vec<u8> {
    let mut unpacked = vec![0u8; c.len() * 64]; // 8 bytes * 8 bits per byte

    for (j, &value) in c.iter().enumerate().rev() {
        for k in 0..8 {
            let mut b = value >> (k * 8) & 0xFF;

            for i in (0..8).rev() {
                // Process each bit in the byte
                let idx = (j * 64) + 8 * (8 - k) - (8 - i);
                if b & 0x01 == 1 {
                    unpacked[idx] = 255;
                } else {
                    unpacked[idx] = 0;
                }

                b = b >> 1;
            }
        }
    }

    unpacked
}

// Reference: https://github.com/worldcoin/iris-matching/blob/main/hamming/hamming.go#L411
pub fn pack_iris_code(a: &[u8]) -> Vec<u8> {
    let capacity = (a.len() / 8) + if a.len() % 8 > 0 { 1 } else { 0 };
    let mut packed = Vec::with_capacity(capacity);

    let mut b: u8 = 0;
    let mut bit_counter = 0;
    for &item in a {
        if item == 255 {
            b |= 1 << (7 - bit_counter);
        }
        bit_counter += 1;
        if bit_counter == 8 {
            packed.push(b);
            b = 0;
            bit_counter = 0;
        }
    }
    if bit_counter > 0 {
        packed.push(b);
    }

    packed
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::*;

    const UNPACKED: &[u8] = &[
        255, 255, 255, 255, 255, 255, 255, 0, 0, 255, 0, 255, 255, 0, 255, 255,
        255, 255, 255, 0, 255, 255, 255, 255, 0, 255, 255, 255, 255, 255, 0,
        255, 255, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        0, 255, 0, 255, 0, 255, 255, 255, 255, 0, 255, 255, 255, 255, 0, 255,
        0,
    ];

    #[test_case(&[0, 0, 0, 0, 0, 0, 0, 0] => vec![0])]
    #[test_case(&[255, 255, 255, 0, 0, 0, 255, 0] => vec![226])]
    #[test_case(&[255, 255, 255, 255, 255, 255, 255, 255] => vec![255])]
    #[test_case(UNPACKED => vec![254, 91, 239, 125, 135, 254, 175, 122])]
    fn pack(unpacked: &[u8]) -> Vec<u8> {
        let packed = pack_iris_code(&unpacked);
        packed
    }

    #[test]
    fn unpack() {
        let packed = pack_iris_code(UNPACKED);

        let limbs = packed
            .array_chunks::<8>()
            .map(|be| u64::from_be_bytes(*be))
            .collect::<Vec<_>>();

        println!("{:?}", limbs);

        let unpacked = unpack_iris_code64(&limbs);

        assert_eq!(unpacked, UNPACKED);
    }
}
