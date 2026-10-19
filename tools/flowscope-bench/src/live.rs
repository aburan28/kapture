//! Rust L7 reconstruction on the same TLS plaintext observations as Kapture.
//! The Go bridge's typed decoding, process-scope checks and pipe IPC remain
//! inside the measured collector. This is not a standalone TLS capture tool.
use flowscope::{http::HttpExchangeParser, SessionParser, Timestamp};
use std::{collections::HashMap, io::{self, Read, Write}};

fn main() -> io::Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let size: usize = args.get(1).expect("body bytes").parse().unwrap();
    let hash = args.get(2).expect("body SHA256").as_bytes();
    let mut input = io::BufReader::new(io::stdin().lock());
    let mut output = io::BufWriter::new(io::stdout().lock());
    let mut flows: HashMap<String, HttpExchangeParser> = HashMap::new();
    loop {
        let mut header = [0_u8; 7];
        match input.read(&mut header[..1])? {
            0 => break,
            1 => input.read_exact(&mut header[1..])?,
            _ => unreachable!(),
        }
        let flow_len = u16::from_le_bytes(header[..2].try_into().unwrap()) as usize;
        let data_len = u32::from_le_bytes(header[2..6].try_into().unwrap()) as usize;
        assert!(flow_len > 0 && flow_len <= 1024 && data_len <= 16384 && header[6] <= 1);
        let mut flow = vec![0; flow_len]; input.read_exact(&mut flow)?;
        let flow = String::from_utf8(flow).expect("UTF8 flow");
        let mut data = vec![0; data_len]; input.read_exact(&mut data)?;
        assert!(flows.contains_key(&flow) || flows.len() < 256, "flow cap");
        let parser = flows.entry(flow).or_default();
        let mut exchanges = Vec::new();
        let ts = Timestamp::new(1700000000, 0);
        if header[6] == 0 { parser.feed_initiator(&data, ts, &mut exchanges); }
        else { parser.feed_responder(&data, ts, &mut exchanges); }
        for exchange in exchanges {
            let request = &exchange.request;
            let id = request.header("x-bench-id").unwrap_or_default();
            let phase = request.header("x-bench-phase") == Some(b"measure".as_slice());
            let number: u64 = std::str::from_utf8(id).unwrap_or("").parse().unwrap_or(0);
            let valid = request.method_str() == Some("POST") && request.path_str() == Some("/bench")
                && request.body.len() == size && request.body.iter().all(|b| *b == b'x')
                && exchange.response.as_ref().is_some_and(|response| response.status == 204
                    && response.header("x-bench-id") == Some(id)
                    && response.header("x-bench-sha256") == Some(hash));
            // No payload is logged. Include the response pipe in the measured
            // adapter cost so the Go parent can verify unique completed IDs.
            output.write_all(&number.to_le_bytes())?;
            output.write_all(&[u8::from(valid), u8::from(phase)])?;
        }
        output.flush()?;
    }
    output.flush()
}
