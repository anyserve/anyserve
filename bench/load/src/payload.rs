#[derive(Clone, Copy, clap::ValueEnum)]
pub(super) enum Pattern {
    Repeat,
    Random,
}

impl Pattern {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Repeat => "repeat",
            Self::Random => "random",
        }
    }

    pub(super) fn algorithm(self) -> &'static str {
        match self {
            Self::Repeat => "repeat-x",
            Self::Random => "splitmix64-v1-le",
        }
    }

    pub(super) fn generate(self, size: usize, seed: u64) -> Vec<u8> {
        if matches!(self, Self::Repeat) {
            return vec![b'x'; size];
        }
        // Stable across platforms/builds; benchmark data, not cryptographic randomness.
        let mut state = seed;
        let mut bytes = vec![0; size];
        for chunk in bytes.chunks_mut(8) {
            state = state.wrapping_add(0x9e3779b97f4a7c15);
            let mut value = state;
            value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
            value ^= value >> 31;
            chunk.copy_from_slice(&value.to_le_bytes()[..chunk.len()]);
        }
        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::Pattern;

    #[test]
    fn payloads_are_reproducible_and_preserve_partial_words() {
        let bytes = Pattern::Random.generate(1025, 0);
        assert_eq!(
            &bytes[..16],
            &[
                0xaf, 0xcd, 0x1d, 0x7b, 0x39, 0xa8, 0x20, 0xe2, 0xf4, 0x65, 0xb9, 0xa1, 0x6a, 0x9e,
                0x78, 0x6e
            ]
        );
        for size in [0, 1, 7, 8, 9, 256, 1025] {
            assert_eq!(Pattern::Random.generate(size, 0), bytes[..size]);
            assert_eq!(Pattern::Repeat.generate(size, u64::MAX), vec![b'x'; size]);
        }
        assert_ne!(bytes, Pattern::Random.generate(1025, 1));
        assert_ne!(bytes, Pattern::Random.generate(1025, u64::MAX));
    }
}
