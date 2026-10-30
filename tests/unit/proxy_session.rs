use super::*;

#[test]
fn tunnel_failures_keep_their_public_transaction_category() {
    for (error, expected) in [
        (
            TunnelError::Directory(TransactionError::Timeout),
            TransactionError::Timeout,
        ),
        (TunnelError::Backpressure, TransactionError::Limit),
        (TunnelError::Busy, TransactionError::Limit),
        (TunnelError::ConnectStatus(1), TransactionError::Rejected),
        (TunnelError::Invalid, TransactionError::Malformed),
        (TunnelError::Protocol, TransactionError::Malformed),
    ] {
        assert_eq!(tunnel_error(error), expected);
    }
}
