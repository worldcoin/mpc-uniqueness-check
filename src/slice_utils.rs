pub fn rotate_slice<T>(s: &mut [T], mut rot: i64)
where
    T: Clone,
{
    let l = s.len();
    if l == 0 || rot == 0 {
        return;
    }

    // Normalize rotation: if negative, convert to equivalent positive rotation
    if rot < 0 {
        rot += l as i64;
    }
    let rot = (rot as usize) % l;

    if rot == 0 {
        return;
    }

    // Reverse the whole slice first
    reverse_slice(s, 0, l);

    // Then reverse the first part (which was the end of the original slice)
    reverse_slice(s, 0, rot);

    // Finally, reverse the second part (which was the beginning of the original slice)
    reverse_slice(s, rot, l);
}

// Helper function to reverse a part of the slice in place
fn reverse_slice<T>(s: &mut [T], start: usize, end: usize)
where
    T: Clone,
{
    let (mut i, mut j) = (start, end - 1);
    while i < j {
        s.swap(i, j);
        i += 1;
        j -= 1;
    }
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::*;

    #[test_case(&[1, 2, 3], 0 => vec![1, 2, 3] ; "No rotation")]
    #[test_case(&[1, 2, 3], 1 => vec![3, 1, 2] ; "Rotate right once")]
    #[test_case(&[1, 2, 3], 2 => vec![2, 3, 1] ; "Rotate right twice")]
    #[test_case(&[1, 2, 3], -1 => vec![2, 3, 1] ; "Rotate left once")]
    #[test_case(&[1, 2, 3], -2 => vec![3, 1, 2] ; "Rotate left twice")]
    #[test_case(&[1, 2, 3], 3 => vec![1, 2, 3] ; "Full rotation")]
    #[test_case(&[1, 2, 3], 4 => vec![3, 1, 2] ; "Full rotation plus one")]
    fn rotation_slice(s: &[u8], rot: i64) -> Vec<u8> {
        let mut s = s.to_vec();

        rotate_slice(&mut s, rot);

        s
    }
}
