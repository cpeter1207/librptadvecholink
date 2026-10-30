//! Loopback standard Proxy login/directory framing shared by owner tests.
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    time::{Duration, Instant},
};

pub(crate) fn receive(socket: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0; 9];
    socket.read_exact(&mut header).unwrap();
    let mut payload = vec![0; u32::from_le_bytes(header[5..].try_into().unwrap()) as usize];
    socket.read_exact(&mut payload).unwrap();
    (header[0], payload)
}
pub(crate) fn send(socket: &mut TcpStream, kind: u8, address: [u8; 4], payload: &[u8]) {
    let mut packet = vec![kind];
    packet.extend(address);
    packet.extend((payload.len() as u32).to_le_bytes());
    packet.extend(payload);
    socket.write_all(&packet).unwrap();
}
pub(crate) fn wait(mut ready: impl FnMut() -> bool) -> bool {
    let end = Instant::now() + Duration::from_secs(3);
    while !ready() {
        if Instant::now() >= end {
            return false;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    true
}
pub(crate) fn authenticated(listener: &TcpListener) -> TcpStream {
    let (mut socket, _) = listener.accept().unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    socket.write_all(b"12345678").unwrap();
    socket.read_exact(&mut [0; 24]).unwrap(); // Seven-character callsign, newline, digest.
    socket
}
pub(crate) fn begin_directory(socket: &mut TcpStream) -> Vec<u8> {
    receive_directory(socket, 1);
    send(socket, 4, [0; 4], &[0; 4]);
    receive_directory(socket, 2)
}
pub(crate) fn receive_directory(socket: &mut TcpStream, expected: u8) -> Vec<u8> {
    loop {
        let (kind, payload) = receive(socket);
        // The one Proxy stream multiplexes directory TCP and QSO UDP records.
        if !matches!(kind, 5 | 6) {
            assert_eq!(kind, expected);
            return payload;
        }
    }
}
pub(crate) fn end_directory(socket: &mut TcpStream, response: &[u8]) {
    send(socket, 2, [0; 4], response);
    receive_directory(socket, 3);
    send(socket, 3, [0; 4], &[]);
}
pub(crate) fn registered(listener: &TcpListener) -> TcpStream {
    let mut socket = authenticated(listener);
    for response in [
        b"OK".as_slice(),
        b"@@@\n1\nW1XYZ-R\nPeer\n123456\n127.0.0.2\n+++\n",
    ] {
        begin_directory(&mut socket);
        end_directory(&mut socket, response);
    }
    socket
}
