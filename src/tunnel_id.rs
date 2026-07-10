use bucky_raw_codec::{RawDecode, RawEncode};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const TUNNEL_ID_EPOCH_MILLIS: u128 = 1_577_577_600_000;
const TUNNEL_ID_TICK_MILLIS: u128 = 100;
const TUNNEL_ID_SPACE: u128 = u32::MAX as u128 + 1;

#[derive(Clone, Copy, PartialEq, Eq, Debug, RawEncode, RawDecode)]
pub struct TunnelId(u32);

impl TunnelId {
    pub fn value(&self) -> u32 {
        self.0
    }

    fn from_unix_millis(now_millis: u128) -> u32 {
        let elapsed_millis = now_millis.saturating_sub(TUNNEL_ID_EPOCH_MILLIS);
        let ticks = elapsed_millis / TUNNEL_ID_TICK_MILLIS;
        (ticks % TUNNEL_ID_SPACE) as u32
    }

    // fn time_bits() -> usize {
    //     20
    // }
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
        let now = TunnelId::from_unix_millis(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
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
    use super::{
        TUNNEL_ID_EPOCH_MILLIS, TUNNEL_ID_SPACE, TUNNEL_ID_TICK_MILLIS, TunnelId, TunnelIdGenerator,
    };

    #[test]
    fn time_seed_saturates_before_epoch_and_wraps_without_overflow() {
        assert_eq!(TunnelId::from_unix_millis(0), 0);
        assert_eq!(
            TunnelId::from_unix_millis(TUNNEL_ID_EPOCH_MILLIS + TUNNEL_ID_TICK_MILLIS),
            1
        );
        assert_eq!(
            TunnelId::from_unix_millis(
                TUNNEL_ID_EPOCH_MILLIS + TUNNEL_ID_SPACE * TUNNEL_ID_TICK_MILLIS
            ),
            0
        );
    }

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
}
