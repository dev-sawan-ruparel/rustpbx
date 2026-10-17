//! Per-session bounded delivery. A lost control/event frame fences the session
//! by cancelling its WebSocket instead of silently claiming delivery.
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct BoundedSender<T> {
    sender: mpsc::Sender<T>,
    cancelled: CancellationToken,
}

pub fn channel<T>(
    capacity: usize,
    cancelled: CancellationToken,
) -> (BoundedSender<T>, mpsc::Receiver<T>) {
    let (sender, receiver) = mpsc::channel(capacity);
    (BoundedSender { sender, cancelled }, receiver)
}

impl<T> BoundedSender<T> {
    pub fn send(&self, value: T) -> Result<(), mpsc::error::TrySendError<T>> {
        let result = self.sender.try_send(value);
        if result.is_err() {
            self.cancelled.cancel();
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_overflow_closes_only_the_slow_session() {
        let slow_cancel = CancellationToken::new();
        let healthy_cancel = CancellationToken::new();
        let (slow, mut slow_rx) = channel(2, slow_cancel.clone());
        let (healthy, mut healthy_rx) = channel(2, healthy_cancel.clone());
        slow.send("first").unwrap();
        slow.send("second").unwrap();
        assert!(slow.send("overflow").is_err());
        assert!(slow_cancel.is_cancelled());
        assert!(!healthy_cancel.is_cancelled());
        assert_eq!(slow_rx.len(), 2);
        assert_eq!(slow_rx.try_recv().unwrap(), "first");
        healthy.send("healthy").unwrap();
        assert_eq!(healthy_rx.try_recv().unwrap(), "healthy");
    }

    #[test]
    fn dropped_receiver_cancels_its_session() {
        let cancel = CancellationToken::new();
        let (sender, receiver) = channel(1, cancel.clone());
        drop(receiver);
        assert!(sender.send("undeliverable").is_err());
        assert!(cancel.is_cancelled());
    }
}
