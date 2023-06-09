/// Flavor of Engine.IO transport.
#[derive(Clone, Eq, PartialEq)]
pub enum TransportType {
    /// Handshakes with polling, upgrades if possible
    Any,
    /// Handshakes with websocket. Does not use polling.
    Websocket,
    /// Handshakes with polling, errors if upgrade fails
    WebsocketUpgrade,
    /// Handshakes with polling
    Polling,
}
