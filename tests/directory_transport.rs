//! Loopback directory exchanges prove partial I/O and completion without remote EOF.

use rptadv_echolink::transport::{ReplyKind, TransactionError, directory_exchange};
use std::{future::Future, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

fn run(test: impl Future<Output = ()>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(test);
}

async fn reply(bytes: Vec<u8>, kind: ReplyKind) -> Result<Vec<u8>, TransactionError> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0; 3];
        stream.read_exact(&mut request).await.unwrap();
        assert_eq!(&request, b"F0\r");
        for byte in bytes {
            if stream.write_all(&[byte]).await.is_err() {
                return;
            }
            tokio::task::yield_now().await;
        }
        // Completion must be framed; waiting for EOF would hit the operation deadline.
        std::future::pending::<()>().await;
    });
    let result = directory_exchange(address, b"F0\r", kind, Duration::from_secs(2)).await;
    server.abort();
    result
}

#[test]
fn directory_login_accepts_split_reply_without_waiting_for_eof() {
    run(async {
        assert_eq!(
            reply(b"OK".to_vec(), ReplyKind::Login).await.unwrap(),
            b"OK"
        );
        assert_eq!(
            reply(b"NO".to_vec(), ReplyKind::Login).await,
            Err(TransactionError::Rejected)
        );
    });
}

#[test]
fn directory_tcp_plain_compressed_and_delta_complete_at_their_own_boundaries() {
    run(async {
        use std::io::Write;
        let full = b"@@@\n1\nK1ABC-R\nPlace\n123456\n192.0.2.1\n+++\n";
        // +++ in a record's description is not an end marker.
        let delta = b"DDD\n1:next\nK2ABC-R\n+++\n234567\n192.0.2.2\n+++\nK1ABC-R\n+++\n";
        for source in [full.as_slice(), delta] {
            let output = reply(source.to_vec(), ReplyKind::Directory).await.unwrap();
            assert_eq!(output, source.strip_suffix(b"\n").unwrap());
            let mut encoder =
                flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
            encoder.write_all(source).unwrap();
            let mut compressed = vec![0, 1, 2, 3];
            compressed.extend(encoder.finish().unwrap());
            assert_eq!(
                reply(compressed.clone(), ReplyKind::Directory)
                    .await
                    .unwrap(),
                compressed
            );
        }
    });
}

#[test]
fn directory_tcp_rejects_oversized_fields_and_expired_operations() {
    run(async {
        let mut oversized = b"@@@\n1\n".to_vec();
        oversized.extend(vec![b'A'; 1025]);
        oversized.push(b'\n');
        assert_eq!(
            reply(oversized, ReplyKind::Directory).await,
            Err(TransactionError::Limit)
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let result = directory_exchange(
            address,
            b"F0\r",
            ReplyKind::Login,
            Duration::from_millis(20),
        )
        .await;
        assert_eq!(result, Err(TransactionError::Timeout));
    });
}

#[test]
fn directory_tcp_early_eof_does_not_fabricate_a_successful_snapshot() {
    run(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 3];
            stream.read_exact(&mut request).await.unwrap();
            stream
                .write_all(b"@@@\n1\nK1ABC-R\nPlace\n123456\n192.0.2.1\n")
                .await
                .unwrap();
        });
        assert_eq!(
            directory_exchange(
                address,
                b"F0\r",
                ReplyKind::Directory,
                Duration::from_secs(2)
            )
            .await,
            Err(TransactionError::Truncated)
        );
        server.await.unwrap();
    });
}

#[test]
fn directory_connect_failure_preserves_its_os_error_category() {
    run(async {
        assert!(matches!(
            directory_exchange(
                "127.0.0.1:0".parse().unwrap(),
                b"F0\r",
                ReplyKind::Directory,
                Duration::from_secs(2)
            )
            .await,
            Err(TransactionError::Io(_))
        ));
    });
}
