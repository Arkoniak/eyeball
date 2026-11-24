use std::{
    hash::{Hash, Hasher},
    mem,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock, Weak,
    },
    task::Waker,
};

/// A subscriber's place for the waker of the task waiting on it.
///
/// The subscriber owns its slot and the state holds it weakly, so the waker
/// of a subscriber dropped while waiting is dropped with the subscriber. A
/// waker keeps its task alive (in tokio, the task's allocation), and without
/// this the waker of a cancelled task stayed in the state until the next
/// update, which for a value that rarely changes is never.
pub(crate) type WakerSlot = Mutex<Option<Waker>>;

/// The fewest slots at which dead ones are pruned.
const PRUNE_AT_LEAST: usize = 16;

/// The slots of the subscribers waiting for an update.
#[derive(Debug)]
struct Wakers {
    slots: Vec<Weak<WakerSlot>>,
    /// The length at which the slots of dropped subscribers are pruned next.
    prune_at: usize,
}

impl Default for Wakers {
    fn default() -> Self {
        Self { slots: Vec::new(), prune_at: PRUNE_AT_LEAST }
    }
}

impl Wakers {
    /// Takes the wakers of the subscribers still alive, leaving no slot.
    fn take(&mut self) -> Vec<Waker> {
        mem::take(&mut self.slots)
            .into_iter()
            .filter_map(|slot| slot.upgrade())
            .filter_map(|slot| slot.lock().unwrap().take())
            .collect()
    }
}

#[derive(Debug)]
pub struct ObservableState<T> {
    /// The inner value.
    value: T,

    /// The version of the value.
    ///
    /// Starts at 1 and is incremented by 1 each time the value is updated.
    /// When the observable is dropped, this is set to 0 to indicate no further
    /// updates will happen.
    version: AtomicU64,

    /// The waker slots of the subscribers waiting for an update.
    ///
    /// This is part of `ObservableState` and uses extra locking so that it is
    /// guaranteed that it's only updated by subscribers while the value is
    /// locked for reading. This way, it is guaranteed that between a subscriber
    /// reading the value and adding a waker because the value hasn't changed
    /// yet, no updates to the value could have happened.
    wakers: RwLock<Wakers>,
}

impl<T> ObservableState<T> {
    pub(crate) fn new(value: T) -> Self {
        Self { value, version: AtomicU64::new(1), wakers: Default::default() }
    }

    /// Get a reference to the inner value.
    pub(crate) fn get(&self) -> &T {
        &self.value
    }

    /// Get the current version of the inner value.
    pub(crate) fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Puts `waker` into the subscriber's `slot`, and the slot among the
    /// waiting ones unless it is there already.
    pub(crate) fn add_waker(&self, slot: &Arc<WakerSlot>, waker: Waker) {
        // A slot holds a waker exactly while it is among the waiting ones:
        // taking the wakers empties both.
        let waiting = slot.lock().unwrap().replace(waker).is_some();
        if waiting {
            return;
        }
        let mut wakers = self.wakers.write().unwrap();
        if wakers.slots.len() >= wakers.prune_at {
            wakers.slots.retain(|slot| slot.strong_count() > 0);
            wakers.prune_at = (wakers.slots.len() * 2).max(PRUNE_AT_LEAST);
        }
        wakers.slots.push(Arc::downgrade(slot));
    }

    pub(crate) fn set(&mut self, value: T) -> T {
        let result = mem::replace(&mut self.value, value);
        self.incr_version_and_wake();
        result
    }

    pub(crate) fn set_if_not_eq(&mut self, value: T) -> Option<T>
    where
        T: PartialEq,
    {
        if self.value != value {
            Some(self.set(value))
        } else {
            None
        }
    }

    pub(crate) fn set_if_hash_not_eq(&mut self, value: T) -> Option<T>
    where
        T: Hash,
    {
        if hash(&self.value) != hash(&value) {
            Some(self.set(value))
        } else {
            None
        }
    }

    pub(crate) fn update(&mut self, f: impl FnOnce(&mut T)) {
        f(&mut self.value);
        self.incr_version_and_wake();
    }

    pub(crate) fn update_if(&mut self, f: impl FnOnce(&mut T) -> bool) {
        if f(&mut self.value) {
            self.incr_version_and_wake();
        }
    }

    /// "Close" the state – indicate that no further updates will happen.
    pub(crate) fn close(&self) {
        self.version.store(0, Ordering::Release);
        // Clear the backing buffer for the wakers, no new ones will be added.
        wake(self.wakers.write().unwrap().take());
    }

    fn incr_version_and_wake(&mut self) {
        self.version.fetch_add(1, Ordering::Release);
        wake(self.wakers.get_mut().unwrap().take());
    }
}

fn hash<T: Hash>(value: &T) -> u64 {
    use std::collections::hash_map::DefaultHasher;

    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn wake<I>(wakers: I)
where
    I: IntoIterator<Item = Waker>,
    I::IntoIter: ExactSizeIterator,
{
    let iter = wakers.into_iter();
    #[cfg(feature = "tracing")]
    {
        let num_wakers = iter.len();
        if num_wakers > 0 {
            tracing::debug!("Waking up {num_wakers} waiting subscribers");
        } else {
            tracing::debug!("No wakers");
        }
    }
    for waker in iter {
        waker.wake();
    }
}
