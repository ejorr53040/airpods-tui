//! Apple RemoteHID over AACP: the sensor channel that carries head motion.
//!
//! Wire format from MousePods by Matteo Gaudioso (starman-tech),
//! <https://github.com/starman-tech/mousepods>, `docs/DISCOVERIES.md` and
//! `docs/REMOTEHID-PROTOCOL.md` (findings CC BY 4.0).

use crate::bluetooth::aacp::HeadMotionSample;

/// AACP header, opcode 0x17 (RTBuddy), descriptor 0x00000800 (RemoteHID).
const PREFIX: [u8; 10] = [0x04, 0x00, 0x04, 0x00, 0x17, 0x00, 0x00, 0x08, 0x00, 0x00];
/// Prefix, body length (u16), then the body's own 4-byte header.
const BODY_START: usize = PREFIX.len() + 2;
const BODY_HEADER: usize = 4;
const SUB_HEADER: usize = 4;

const KIND_CONNECT: u8 = 0;
const KIND_INPUT_REPORT: u8 = 2;
const KIND_SET_REPORT: u8 = 3;

/// HID feature report, report id 1: the sensor's report interval in µs.
const FEATURE_REPORT: u8 = 0x02;
const REPORT_INTERVAL: u8 = 0x01;

/// HostConnect: asks the AirPods to announce their sensors. `generation`
/// counts the frames this host sent.
pub fn host_connect(generation: u8) -> Vec<u8> {
    frame(generation, 0, KIND_CONNECT, &[])
}

/// SetReport on feature report 1 (report interval, µs); 0 stops the sensor.
pub fn set_report_interval(generation: u8, device: u8, interval_us: u32) -> Vec<u8> {
    let mut payload = vec![FEATURE_REPORT, REPORT_INTERVAL];
    payload.extend_from_slice(&interval_us.to_le_bytes());
    frame(generation, device, KIND_SET_REPORT, &payload)
}

/// A frame from this host holding one sub-packet.
fn frame(generation: u8, device: u8, kind: u8, payload: &[u8]) -> Vec<u8> {
    let sub_len = SUB_HEADER + payload.len();
    let header = u32::from(device & 0x7F) | ((sub_len as u32 & 0x3FF) << 7) | (u32::from(kind) << 17);
    let mut out = Vec::with_capacity(BODY_START + BODY_HEADER + sub_len);
    out.extend_from_slice(&PREFIX);
    out.extend_from_slice(&((BODY_HEADER + sub_len) as u16).to_le_bytes());
    out.extend_from_slice(&[0x00, generation, 0x00, 0x00]);
    out.extend_from_slice(&header.to_le_bytes());
    out.extend_from_slice(payload);
    out
}

#[derive(Debug, Clone, PartialEq)]
pub enum SubPacket<'a> {
    /// A sensor announcing itself, named by its plist's `AccessoryService`.
    DeviceConnect { device: u8, service: &'a str },
    /// A report from a sensor: report-type byte, then the sensor's data.
    InputReport { device: u8, report: &'a [u8] },
    /// Anything this host does not act on (replies, disconnects).
    Other { device: u8, kind: u8 },
}

/// The sub-packets of a RemoteHID frame, or none if `frame` is not one.
pub fn parse_frame(frame: &[u8]) -> Vec<SubPacket<'_>> {
    let mut subs = Vec::new();
    if !frame.starts_with(&PREFIX) || frame.len() < BODY_START {
        return subs;
    }
    let declared = u16::from_le_bytes([frame[10], frame[11]]) as usize;
    let body = &frame[BODY_START..frame.len().min(BODY_START + declared)];
    let mut rest = body.get(BODY_HEADER..).unwrap_or_default();
    while rest.len() >= SUB_HEADER {
        let header = u32::from_le_bytes([rest[0], rest[1], rest[2], rest[3]]);
        let device = (header & 0x7F) as u8;
        let len = ((header >> 7) & 0x3FF) as usize;
        let kind = ((header >> 17) & 0x7) as u8;
        if len < SUB_HEADER || len > rest.len() {
            break;
        }
        let payload = &rest[SUB_HEADER..len];
        let other = SubPacket::Other { device, kind };
        subs.push(match kind {
            KIND_CONNECT => accessory_service(payload)
                .map_or(other, |service| SubPacket::DeviceConnect { device, service }),
            KIND_INPUT_REPORT => SubPacket::InputReport {
                device,
                report: payload,
            },
            _ => other,
        });
        rest = &rest[len..];
    }
    subs
}

/// The `AccessoryService` name in a DeviceConnect's plist.
fn accessory_service(payload: &[u8]) -> Option<&str> {
    const KEY: &[u8] = b"<key>AccessoryService</key><string>";
    let start = payload.windows(KEY.len()).position(|w| w == KEY)? + KEY.len();
    let len = payload[start..].iter().position(|&b| b == b'<')?;
    std::str::from_utf8(&payload[start..start + len]).ok()
}

/// The head-motion sample in a `devmotion6` input report: a report-type
/// byte, then the 58-byte motion block (`[1..9]` u64 timestamp in ns,
/// `[20..26]` the attitude quaternion's vector part as Q15).
pub fn decode_motion(report: &[u8]) -> Option<HeadMotionSample> {
    let block = report.get(1..27)?;
    let timestamp_ns = u64::from_le_bytes(block[1..9].try_into().ok()?);
    let q15 = |at: usize| f32::from(i16::from_le_bytes([block[at], block[at + 1]])) / 32768.0;
    let (x, y, z) = (q15(20), q15(22), q15(24));
    let w = (1.0 - x * x - y * y - z * z).max(0.0).sqrt();
    Some(HeadMotionSample {
        timestamp_ns,
        quaternion: [w, x, y, z],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A frame captured from AirPods 3 (MousePods `captures/airpods3_glide_raw.json`,
    /// CC BY 4.0): an `accel` report from device 4, then a `devmotion6`
    /// report from device 7.
    const CAPTURED_FRAME: &str = "04000400170000080000640000725e008410040000018ffdffff212d000050ecfdffd309000000705d5b670c1000000000871f040000011875f5bd770600000300035b670c10000000004f01312dc10847ff0400b9001e001d001e007bfdaa0000fee102ecff1dfd5e66743d41cb0000";

    fn assert_close(actual: [f32; 4], expected: [f32; 4]) {
        for (a, e) in actual.iter().zip(expected) {
            assert!((a - e).abs() < 1e-6, "{actual:?} != {expected:?}");
        }
    }

    #[test]
    fn a_captured_frame_yields_each_report_and_its_motion_sample() {
        let frame = hex::decode(CAPTURED_FRAME).unwrap();
        let subs = parse_frame(&frame);
        assert_eq!(subs.len(), 2);
        assert!(matches!(subs[0], SubPacket::InputReport { device: 4, report } if report.len() == 29));
        let SubPacket::InputReport { device: 7, report } = subs[1] else {
            panic!("expected a devmotion6 report, got {:?}", subs[1]);
        };
        let sample = decode_motion(report).unwrap();
        assert_eq!(sample.timestamp_ns, 7_111_357_855_000);
        // x, y, z = 335, 11569, 2241 / 32768; w = sqrt(1 - x² - y² - z²).
        assert_close(
            sample.quaternion,
            [0.933_042_6, 0.010_223_389, 0.353_057_86, 0.068_389_89],
        );
    }

    /// A frame from the AirPods around the given sub-packets.
    fn incoming(subs: &[u8]) -> Vec<u8> {
        let mut f = vec![0x04, 0x00, 0x04, 0x00, 0x17, 0x00, 0x00, 0x08, 0x00, 0x00];
        f.extend_from_slice(&((subs.len() + 4) as u16).to_le_bytes());
        f.extend_from_slice(&[0x00, 0x01, 0x00, 0x00]);
        f.extend_from_slice(subs);
        f
    }

    /// A devmotion6 DeviceConnect: the plist, then the start of a HID
    /// report descriptor. 184 payload bytes, so the header is
    /// device 4 | 188 << 7 | kind 0 << 17 = `04 5e 00 00`.
    fn devmotion6_announcement() -> Vec<u8> {
        let mut sub = vec![0x04, 0x5e, 0x00, 0x00];
        sub.extend_from_slice(
            br#"<?xml version="1.0" encoding="UTF-8"?><plist version="1.0"><dict><key>AccessoryService</key><string>devmotion6</string><key>MaxReportSize</key><integer>182</integer></dict></plist>"#,
        );
        sub.extend_from_slice(&[0x05, 0x20, 0x09, 0xe1]);
        sub
    }

    #[test]
    fn an_announcement_names_the_sensor_and_its_device_id() {
        let frame = incoming(&devmotion6_announcement());
        assert_eq!(
            parse_frame(&frame),
            vec![SubPacket::DeviceConnect {
                device: 4,
                service: "devmotion6"
            }]
        );
    }

    #[test]
    fn a_truncated_frame_keeps_only_whole_sub_packets() {
        let mut frame = hex::decode(CAPTURED_FRAME).unwrap();
        // Cut into the devmotion6 report: the accel report before it survives.
        frame.truncate(100);
        let subs = parse_frame(&frame);
        assert_eq!(subs.len(), 1);
        assert!(matches!(subs[0], SubPacket::InputReport { device: 4, .. }));
        // Too short for the length field, or not RemoteHID at all.
        assert!(parse_frame(&frame[..11]).is_empty());
        assert!(parse_frame(&[0x04, 0x00, 0x04, 0x00, 0x06, 0x00, 0x01, 0x00]).is_empty());
    }

    #[test]
    fn a_motion_block_too_short_for_the_attitude_has_no_sample() {
        assert_eq!(decode_motion(&[0x00; 26]), None);
    }

    #[test]
    fn host_connect_is_an_empty_connect_to_device_zero() {
        assert_eq!(
            host_connect(5),
            hex::decode("040004001700000800000800 00050000 00020000".replace(' ', "")).unwrap()
        );
    }

    /// DISCOVERIES §4: SetReport on device 7, feature report 1, 10 000 µs.
    #[test]
    fn set_report_interval_matches_the_documented_example() {
        assert_eq!(
            set_report_interval(9, 7, 10_000),
            hex::decode("040004001700000800000e00 00090000 07050600 0201 10270000".replace(' ', ""))
                .unwrap()
        );
    }
}
