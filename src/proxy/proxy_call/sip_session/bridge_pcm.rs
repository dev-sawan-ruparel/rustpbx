//! Explicit wire encoding used by both production WebSocket bridge directions.
//! This is raw PCM, not the vbox generation-prefixed protocol.

pub(super) const MAX_PCM_FRAME_BYTES: usize = 6400;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum InvalidPcmFrame {
    Empty,
    OddLength,
    TooLarge,
}

pub(super) fn decode_pcm16le(bytes: &[u8]) -> Result<Vec<i16>, InvalidPcmFrame> {
    if bytes.is_empty() {
        return Err(InvalidPcmFrame::Empty);
    }
    if bytes.len() > MAX_PCM_FRAME_BYTES {
        return Err(InvalidPcmFrame::TooLarge);
    }
    if bytes.len() % 2 != 0 {
        return Err(InvalidPcmFrame::OddLength);
    }
    Ok(bytes
        .chunks_exact(2)
        .map(|pair| i16::from_le_bytes([pair[0], pair[1]]))
        .collect())
}

pub(super) fn encode_pcm16le(samples: &[i16]) -> Vec<u8> {
    samples
        .iter()
        .flat_map(|sample| sample.to_le_bytes())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Literal bytes catch two mutually compensating endian errors that a
    // round-trip-only test cannot detect.
    const WIRE: [u8; 12] = [
        0x00, 0x00, 0x01, 0x00, 0xff, 0xff, 0x34, 0x12, 0x00, 0x80, 0xff, 0x7f,
    ];
    const SAMPLES: [i16; 6] = [0, 1, -1, 0x1234, i16::MIN, i16::MAX];

    #[test]
    fn websocket_to_rtp_known_little_endian_fixture() {
        assert_eq!(decode_pcm16le(&WIRE).unwrap(), SAMPLES);
    }

    #[test]
    fn rtp_to_websocket_known_little_endian_fixture() {
        assert_eq!(encode_pcm16le(&SAMPLES), WIRE);
    }

    #[test]
    fn invalid_frames_are_rejected_before_allocating_samples() {
        assert_eq!(decode_pcm16le(&[]), Err(InvalidPcmFrame::Empty));
        assert_eq!(decode_pcm16le(&[0]), Err(InvalidPcmFrame::OddLength));
        assert_eq!(
            decode_pcm16le(&vec![0; MAX_PCM_FRAME_BYTES + 2]),
            Err(InvalidPcmFrame::TooLarge)
        );
        assert_eq!(
            decode_pcm16le(&vec![0; MAX_PCM_FRAME_BYTES]).unwrap().len(),
            3200
        );
    }
}
