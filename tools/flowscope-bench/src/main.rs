use flowscope::{SessionParser, Timestamp, http::HttpExchangeParser};
use std::{hint::black_box, time::Instant};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let size: usize = args.get(1).expect("body bytes").parse().unwrap();
    let iterations: usize = args.get(2).expect("iterations").parse().unwrap();
    assert!(size <= 1 << 20 && iterations > 0);
    let request = format!(
        "POST /bench HTTP/1.1\r\nHost: benchmark\r\nContent-Length: {size}\r\n\r\n{}",
        "x".repeat(size)
    )
    .into_bytes();
    let response = b"HTTP/1.1 204 No Content\r\n\r\n";
    let ts = Timestamp::new(1700000000, 0);
    let mut count = 0;
    let start = Instant::now();
    for _ in 0..iterations {
        let mut parser = HttpExchangeParser::new();
        let mut out = Vec::new();
        parser.feed_initiator(black_box(&request), ts, &mut out);
        parser.feed_responder(black_box(response), ts, &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].request.body.len(), size);
        assert_eq!(out[0].response.as_ref().unwrap().status, 204);
        count += black_box(out.len());
    }
    let elapsed = start.elapsed();
    assert_eq!(count, iterations);
    println!(
        "flowscope,http1,{size},{iterations},{count},{},{:.2}",
        request.len() + response.len(),
        elapsed.as_nanos() as f64 / iterations as f64
    );
}
