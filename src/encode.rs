use crate::distance::EncodedBits;
use crate::template::Template;

/// Generate a [`EncodedBits`] such that values are $\{-1,0,1\}$, representing
/// unset, masked and set.
pub fn encode(template: &Template) -> EncodedBits {
    // Make sure masked-out pattern bits are zero;
    let pattern = &template.pattern & &template.mask;

    // Convert to u16s
    let pattern = EncodedBits::from(&pattern);
    let mask = EncodedBits::from(&template.mask);

    // Preprocessed is (mask - 2 * pattern)
    mask - &pattern - &pattern
}
