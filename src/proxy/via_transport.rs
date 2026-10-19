//! Keep the top Via of an outgoing request on the transport it is sent over.
//!
//! rsipstack builds an in-dialog request's Via before the transport is
//! chosen. With a target locator installed (RustPBX always installs one) and
//! no reusable flow — any dialog with a route set — it falls back to the
//! first listener, normally UDP. A BYE sent over TLS to Telnyx's
//! record-routing proxy therefore carried `Via: SIP/2.0/UDP <ip>:5060`
//! (live, 2026-10-19). RFC 3261 §18.1.1 requires the Via transport and
//! sent-by to name the transport actually used, so this inspector rewrites
//! the top Via of a request whose destination is a reliable transport to the
//! local listener of that transport. The branch and other parameters stay.

use rsipstack::sip::prelude::HeadersExt;
use rsipstack::sip::{Param, SipMessage, ToTypedHeader, Transport};
use rsipstack::transaction::endpoint::MessageInspector;
use rsipstack::transport::SipAddr;

pub struct ViaTransportInspector {
    listeners: Vec<SipAddr>,
}

impl ViaTransportInspector {
    pub fn new(listeners: Vec<SipAddr>) -> Self {
        Self { listeners }
    }

    fn fix_request(&self, req: &mut rsipstack::sip::Request, dest: &SipAddr) {
        let Some(transport) = dest.r#type else { return };
        if transport == Transport::Udp {
            return;
        }
        let Some(listener) = self.listeners.iter().find(|a| a.r#type == Some(transport)) else {
            return;
        };
        let Ok(via) = req.via_header_mut() else { return };
        let _ = via.update_first_value(|value| {
            let mut typed = value.typed()?;
            if typed.transport == transport {
                return Ok(value);
            }
            typed.transport = transport;
            typed.uri = listener.addr.clone().into();
            typed.params.retain(|p| match p {
                Param::Transport(_) => false,
                Param::Other(name, _) => !name.to_string().eq_ignore_ascii_case("alias"),
                _ => true,
            });
            if matches!(transport, Transport::Tcp | Transport::Tls) {
                typed.params.push(Param::Other("alias".into(), None));
            }
            Ok(typed.into())
        });
    }
}

impl MessageInspector for ViaTransportInspector {
    fn before_send(&self, msg: SipMessage, dest: Option<&SipAddr>) -> SipMessage {
        match (msg, dest) {
            (SipMessage::Request(mut req), Some(dest)) => {
                self.fix_request(&mut req, dest);
                SipMessage::Request(req)
            }
            (msg, _) => msg,
        }
    }

    fn after_received(&self, msg: SipMessage, _from: Option<&SipAddr>) -> SipMessage {
        msg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(t: Transport, s: &str) -> SipAddr {
        SipAddr {
            r#type: Some(t),
            addr: rsipstack::sip::HostWithPort::try_from(s).unwrap(),
        }
    }

    fn top_via(msg: &SipMessage) -> String {
        msg.to_string()
            .lines()
            .find(|l| l.starts_with("Via:"))
            .unwrap()
            .to_string()
    }

    // The live BYE (2026-10-19): built with the UDP listener, sent over TLS.
    const BYE: &str = concat!(
        "BYE sip:mod_sofia@10.239.42.224:6000 SIP/2.0\r\n",
        "Via: SIP/2.0/UDP 203.0.113.80:5060;branch=z9hG4bKMUNIG8VhQYIw;rport\r\n",
        "Route: <sip:192.76.120.10:5061;transport=tls;r2=on;lr;ftag=D6XDp4jpmyN4B>\r\n",
        "Route: <sip:10.255.0.1;r2=on;lr;ftag=D6XDp4jpmyN4B>\r\n",
        "From: <sip:+15550100002@203.0.113.80>;tag=pbx1\r\n",
        "To: <sip:+15550100001@sip.telnyx.com>;tag=D6XDp4jpmyN4B\r\n",
        "Call-ID: c1\r\n",
        "CSeq: 2 BYE\r\n",
        "Content-Length: 0\r\n",
        "\r\n"
    );

    fn inspector() -> ViaTransportInspector {
        ViaTransportInspector::new(vec![
            addr(Transport::Udp, "203.0.113.80:5060"),
            addr(Transport::Tls, "203.0.113.80:5061"),
        ])
    }

    #[test]
    fn request_sent_over_tls_gets_the_tls_listener_via() {
        let msg = SipMessage::try_from(BYE).unwrap();
        let out = inspector().before_send(msg, Some(&addr(Transport::Tls, "192.76.120.10:5061")));
        let via = top_via(&out);
        assert!(via.starts_with("Via: SIP/2.0/TLS 203.0.113.80:5061;"), "{via}");
        assert!(via.contains("branch=z9hG4bKMUNIG8VhQYIw"), "{via}");
        assert!(out.to_string().starts_with("BYE sip:mod_sofia@10.239.42.224:6000 SIP/2.0\r\n"));
    }

    #[test]
    fn udp_and_matching_transports_are_untouched() {
        let msg = SipMessage::try_from(BYE).unwrap();
        let out = inspector().before_send(msg, Some(&addr(Transport::Udp, "192.76.120.10:5060")));
        assert_eq!(top_via(&out), "Via: SIP/2.0/UDP 203.0.113.80:5060;branch=z9hG4bKMUNIG8VhQYIw;rport");
        // No listener for the transport: leave it.
        let msg = SipMessage::try_from(BYE).unwrap();
        let out = inspector().before_send(msg, Some(&addr(Transport::Tcp, "192.76.120.10:5060")));
        assert!(top_via(&out).starts_with("Via: SIP/2.0/UDP "));
    }
}
