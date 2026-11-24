//! A subscriber dropped while its task waits for an update drops the task's waker with it.

use std::{
    future::Future,
    pin::pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
};

use eyeball::{Observable, SharedObservable};

/// A waker that counts nothing: its `Arc`'s strong count tells who holds it.
struct Task;

impl Wake for Task {
    fn wake(self: Arc<Self>) {}
}

#[test]
fn shared_subscriber_dropped_while_waiting() {
    let task = Arc::new(Task);
    let waker = Waker::from(task.clone());
    let mut cx = Context::from_waker(&waker);
    let ob = SharedObservable::new(0);

    let mut subscriber = ob.subscribe();
    {
        let next = pin!(subscriber.next());
        assert_eq!(next.poll(&mut cx), Poll::Pending);
    }
    // The task's own `Arc`, the `Waker`, and the clone the observable waits with.
    assert_eq!(Arc::strong_count(&task), 3);

    drop(subscriber);
    assert_eq!(Arc::strong_count(&task), 2);
}

#[test]
fn unique_subscriber_dropped_while_waiting() {
    let task = Arc::new(Task);
    let waker = Waker::from(task.clone());
    let mut cx = Context::from_waker(&waker);
    let ob = Observable::new(0);

    let mut subscriber = Observable::subscribe(&ob);
    {
        let next = pin!(subscriber.next());
        assert_eq!(next.poll(&mut cx), Poll::Pending);
    }
    assert_eq!(Arc::strong_count(&task), 3);

    drop(subscriber);
    assert_eq!(Arc::strong_count(&task), 2);
}

#[test]
fn repeated_polls_keep_one_waker() {
    let task = Arc::new(Task);
    let waker = Waker::from(task.clone());
    let mut cx = Context::from_waker(&waker);
    let mut ob = Observable::new(0);

    let mut subscriber = Observable::subscribe(&ob);
    for _ in 0..100 {
        let next = pin!(subscriber.next());
        assert_eq!(next.poll(&mut cx), Poll::Pending);
    }
    assert_eq!(Arc::strong_count(&task), 3);

    Observable::set(&mut ob, 1);
    assert_eq!(Arc::strong_count(&task), 2);
    let next = pin!(subscriber.next());
    assert_eq!(next.poll(&mut cx), Poll::Ready(Some(1)));
}

#[test]
fn many_dropped_subscribers_are_pruned() {
    let task = Arc::new(Task);
    let waker = Waker::from(task.clone());
    let mut cx = Context::from_waker(&waker);
    let mut ob = Observable::new(0);

    for _ in 0..1000 {
        let mut subscriber = Observable::subscribe(&ob);
        let next = pin!(subscriber.next());
        assert_eq!(next.poll(&mut cx), Poll::Pending);
    }
    assert_eq!(Arc::strong_count(&task), 2);

    // The ones still waiting are woken.
    let mut subscriber = Observable::subscribe(&ob);
    {
        let next = pin!(subscriber.next());
        assert_eq!(next.poll(&mut cx), Poll::Pending);
    }
    Observable::set(&mut ob, 1);
    let next = pin!(subscriber.next());
    assert_eq!(next.poll(&mut cx), Poll::Ready(Some(1)));
}
