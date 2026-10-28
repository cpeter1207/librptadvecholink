//! Bounded, asynchronous directory I/O shared by direct and Proxy stream transports.

pub mod direct;
pub mod driver;
pub mod mailbox;
pub mod routing;

use crate::directory::{MAX_COMPRESSED, MAX_EXPANDED, MAX_LINE};
use flate2::{Decompress, FlushDecompress, Status};
use std::{io::ErrorKind, net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpStream,
};

/// Expected reply determines its real completion boundary, not a read-size guess.
#[derive(Clone, Copy)]
pub enum ReplyKind {
    /// Two-byte registration acknowledgment.
    Login,
    /// Full/delta plain or four-byte-prefixed zlib snapshot.
    Directory,
}

/// Metadata-only failure; never includes a request, password or server response body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionError {
    /// Connect plus I/O exceeded the one operation deadline.
    Timeout,
    /// Transport failure with its OS category retained.
    Io(ErrorKind),
    /// Connection ended before the framed response completed.
    Truncated,
    /// Registration was explicitly rejected.
    Rejected,
    /// Header or compressed stream is invalid.
    Malformed,
    /// Compressed, expanded or field budget exhausted before unbounded allocation.
    Limit,
}

/// One direct TCP transaction; no public service is contacted except the supplied endpoint.
pub async fn directory_exchange(
    address: SocketAddr,
    request: &[u8],
    kind: ReplyKind,
    deadline: Duration,
) -> Result<Vec<u8>, TransactionError> {
    tokio::time::timeout(deadline, async {
        let stream = TcpStream::connect(address).await.map_err(io_error)?;
        exchange(stream, request, kind).await
    })
    .await
    .map_err(|_| TransactionError::Timeout)?
}

/// Perform the same framed exchange over an already connected Proxy directory stream.
pub async fn exchange_on_stream<S>(
    stream: S,
    request: &[u8],
    kind: ReplyKind,
    deadline: Duration,
) -> Result<Vec<u8>, TransactionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    tokio::time::timeout(deadline, exchange(stream, request, kind))
        .await
        .map_err(|_| TransactionError::Timeout)?
}

async fn exchange<S>(
    mut stream: S,
    request: &[u8],
    kind: ReplyKind,
) -> Result<Vec<u8>, TransactionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream.write_all(request).await.map_err(io_error)?;
    stream.flush().await.map_err(io_error)?;
    let mut reader = BufReader::new(stream);
    if matches!(kind, ReplyKind::Login) {
        let mut reply = [0; 2];
        reader.read_exact(&mut reply).await.map_err(io_error)?;
        return if &reply == b"OK" {
            Ok(reply.to_vec())
        } else {
            Err(TransactionError::Rejected)
        };
    }
    let mut prefix = [0; 4];
    reader.read_exact(&mut prefix).await.map_err(io_error)?;
    match &prefix {
        b"@@@\n" | b"DDD\n" => read_plain(&mut reader, prefix.to_vec(), &prefix == b"DDD\n").await,
        _ => read_compressed(&mut reader, prefix.to_vec()).await,
    }
}

async fn read_plain<S: AsyncRead + Unpin>(
    reader: &mut BufReader<S>,
    mut output: Vec<u8>,
    partial: bool,
) -> Result<Vec<u8>, TransactionError> {
    append(&mut output, &read_line(reader, false).await?, MAX_EXPANDED)?;
    let mut fields = 0;
    let mut deleting = false;
    loop {
        let line = read_line(reader, fields == 0 && (!partial || deleting)).await?;
        let contents = line.strip_suffix(b"\n").unwrap_or(&line);
        let contents = contents.strip_suffix(b"\r").unwrap_or(contents);
        append(&mut output, &line, MAX_EXPANDED)?;
        if fields != 0 {
            fields -= 1;
        } else if contents == b"+++" {
            if !partial || deleting {
                return Ok(output);
            }
            deleting = true;
        } else if !deleting {
            fields = 3;
        }
    }
}

async fn read_line<S: AsyncRead + Unpin>(
    reader: &mut BufReader<S>,
    terminal_marker: bool,
) -> Result<Vec<u8>, TransactionError> {
    let mut line = Vec::new();
    loop {
        let input = reader.fill_buf().await.map_err(io_error)?;
        if input.is_empty() {
            return Err(TransactionError::Truncated);
        }
        // EchoLib accepts a final three-byte marker without a trailing newline.
        // Only a callsign/deletion boundary can end the response, never a description.
        let needed = 3usize.saturating_sub(line.len());
        if terminal_marker && needed > 0 && input.len() >= needed {
            let mut prefix = line.clone();
            prefix.extend_from_slice(&input[..needed]);
            if prefix == b"+++" {
                reader.consume(needed);
                return Ok(prefix);
            }
        }
        let count = input
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(input.len(), |position| position + 1);
        if line.len() + count > MAX_LINE + 1 {
            return Err(TransactionError::Limit);
        }
        let done = input[count - 1] == b'\n';
        line.extend_from_slice(&input[..count]);
        reader.consume(count);
        if done {
            return Ok(line);
        }
    }
}

async fn read_compressed<S: AsyncRead + Unpin>(
    reader: &mut BufReader<S>,
    mut output: Vec<u8>,
) -> Result<Vec<u8>, TransactionError> {
    let mut decoder = Decompress::new(true);
    let mut input = [0; 8192];
    loop {
        let count = reader.read(&mut input).await.map_err(io_error)?;
        if count == 0 {
            return Err(TransactionError::Truncated);
        }
        append(&mut output, &input[..count], MAX_COMPRESSED)?;
        loop {
            let before_in = decoder.total_in();
            let before_out = decoder.total_out();
            let mut decoded = [0; 8192];
            let status = decoder
                .decompress(
                    &output[4 + before_in as usize..],
                    &mut decoded,
                    FlushDecompress::None,
                )
                .map_err(|_| TransactionError::Malformed)?;
            if decoder.total_out() > MAX_EXPANDED as u64 {
                return Err(TransactionError::Limit);
            }
            if status == Status::StreamEnd {
                return if decoder.total_in() == (output.len() - 4) as u64 {
                    Ok(output)
                } else {
                    Err(TransactionError::Malformed)
                };
            }
            if decoder.total_in() == before_in && decoder.total_out() == before_out {
                break;
            }
        }
    }
}

fn append(output: &mut Vec<u8>, input: &[u8], maximum: usize) -> Result<(), TransactionError> {
    if input.len() > maximum - output.len() {
        return Err(TransactionError::Limit);
    }
    output.extend_from_slice(input);
    Ok(())
}

fn io_error(error: std::io::Error) -> TransactionError {
    if error.kind() == ErrorKind::UnexpectedEof {
        TransactionError::Truncated
    } else {
        TransactionError::Io(error.kind())
    }
}
