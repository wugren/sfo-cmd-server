use bucky_raw_codec::{RawDecode, RawEncode};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Ord, PartialEq, Eq, Debug, RawEncode, RawDecode)]
pub struct TunnelId(u32);

impl TunnelId {
    pub fn value(&self) -> u32 {
        self.0
    }

    fn now(_now: u64) -> u32 {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;
        let since_2021 = Duration::from_secs((50 * 365 + 9) * 24 * 3600).as_secs() as u32;
        // TODO: 用10年？
        (now - since_2021) * 10
    }

    // fn time_bits() -> usize {
    //     20
    // }
}

impl PartialOrd for TunnelId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.0 == 0 || other.0 == 0 {
            self.0.partial_cmp(&other.0)
        } else if (std::cmp::max(self.0, other.0) - std::cmp::min(self.0, other.0)) > (u32::MAX / 2)
        {
            Some(if self.0 > other.0 {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            })
        } else {
            self.0.partial_cmp(&other.0)
        }
    }
}

impl Default for TunnelId {
    fn default() -> Self {
        Self(0)
    }
}

impl From<u32> for TunnelId {
    fn from(v: u32) -> Self {
        Self(v)
    }
}

impl Hash for TunnelId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u32(self.0)
    }
}

pub struct TunnelIdGenerator {
    cur: AtomicU32,
}

impl From<TunnelId> for TunnelIdGenerator {
    fn from(init: TunnelId) -> Self {
        Self {
            cur: AtomicU32::new(init.value()),
        }
    }
}

impl TunnelIdGenerator {
    pub fn new() -> Self {
        let now = TunnelId::now(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        );
        Self {
            cur: AtomicU32::new(now),
        }
    }

    pub fn generate(&self) -> TunnelId {
        let v = self.cur.fetch_add(1, Ordering::SeqCst);
        if v == 0 {
            TunnelId(self.cur.fetch_add(1, Ordering::SeqCst))
        } else {
            TunnelId(v)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{TunnelId, TunnelIdGenerator};

    #[test]
    fn generator_skips_zero_and_increments() {
        let generator = TunnelIdGenerator::from(TunnelId::from(0));
        let first = generator.generate();
        let second = generator.generate();
        let third = generator.generate();

        assert_eq!(first.value(), 1);
        assert_eq!(second.value(), 2);
        assert_eq!(third.value(), 3);
    }

    #[test]
    fn partial_ord_wrap_around_behavior() {
        let near_max = TunnelId::from(u32::MAX - 10);
        let small = TunnelId::from(10);
        assert!(near_max < small);
        assert!(small > near_max);
    }

    #[test]
    fn partial_ord_zero_follows_natural_order() {
        let zero = TunnelId::from(0);
        let one = TunnelId::from(1);
        assert!(zero < one);
        assert!(one > zero);
    }
}
