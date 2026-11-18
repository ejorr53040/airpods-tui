use crate::tui::app::{AppEvent, DeviceCommand};
use log::{error, info};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{RwLock, broadcast, mpsc};

pub fn socket_path() -> std::io::Result<PathBuf> {
    Ok(crate::utils::runtime_dir()?.join("airpods-tui.sock"))
}

async fn write_msg(stream: &mut (impl AsyncWriteExt + Unpin), data: &[u8]) -> std::io::Result<()> {
    let len = (data.len() as u32).to_be_bytes();
    stream.write_all(&len).await?;
    stream.write_all(data).await?;
    stream.flush().await
}

async fn read_msg(stream: &mut (impl AsyncReadExt + Unpin)) -> std::io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 16 * 1024 * 1024 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "message too large",
        ));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

/// State snapshot maintained by the daemon for replaying to new clients.
pub type StateSnapshot = Arc<RwLock<Vec<AppEvent>>>;

/// Build a fresh snapshot from a stream of AppEvents.
/// Keeps the latest DeviceConnected + all AACPEvents per device.
pub fn update_snapshot(snapshot: &mut Vec<AppEvent>, event: &AppEvent) {
    match event {
        AppEvent::DeviceConnected { mac, .. } => {
            // Remove old events for this device and re-add
            snapshot.retain(|e| match e {
                AppEvent::DeviceConnected { mac: m, .. } => m != mac,
                AppEvent::DeviceNearby { mac: m, .. } => m != mac,
                AppEvent::AACPEvent(m, _) => m != mac,
                _ => true,
            });
            snapshot.push(event.clone());
        }
        AppEvent::DeviceNearby { mac, .. } => {
            // A live session outranks a broadcast.
            if snapshot
                .iter()
                .any(|e| matches!(e, AppEvent::DeviceConnected { mac: m, .. } if m == mac))
            {
                return;
            }
            snapshot.retain(|e| !matches!(e, AppEvent::DeviceNearby { mac: m, .. } if m == mac));
            snapshot.push(event.clone());
        }
        AppEvent::DeviceDisconnected(mac) => {
            snapshot.retain(|e| match e {
                AppEvent::DeviceConnected { mac: m, .. } => m != mac,
                AppEvent::DeviceNearby { mac: m, .. } => m != mac,
                AppEvent::AACPEvent(m, _) => m != mac,
                AppEvent::DeviceDisconnected(m) => m != mac,
                _ => true,
            });
        }
        AppEvent::AACPEvent(mac, aacp_event) => {
            // For control commands / battery, replace previous of same variant per device
            use crate::bluetooth::aacp::AACPEvent as AE;
            match &**aacp_event {
                AE::BatteryInfo(new_infos) => {
                    // Preserve last known "good" case battery when the new event
                    // reports Case as Disconnected (case was closed).  Without this,
                    // new IPC clients that replay the snapshot would lose the case
                    // level because handle_aacp_event skips Disconnected case entries.
                    use crate::bluetooth::aacp::{BatteryComponent, BatteryStatus};
                    let new_has_case = new_infos.iter().any(|b| {
                        b.component == BatteryComponent::Case
                            && b.status != BatteryStatus::Disconnected
                    });
                    if !new_has_case {
                        // Find the previous case battery entry from the old snapshot
                        let prev_case = snapshot.iter().find_map(|e| {
                            if let AppEvent::AACPEvent(m, ae) = e
                                && m == mac
                                && let AE::BatteryInfo(prev) = &**ae
                            {
                                prev.iter()
                                    .find(|b| {
                                        b.component == BatteryComponent::Case
                                            && b.status != BatteryStatus::Disconnected
                                    })
                                    .cloned()
                            } else {
                                None
                            }
                        });
                        if let Some(case_info) = prev_case {
                            // Rebuild the event with the preserved case entry
                            let mut merged = new_infos.clone();
                            merged.retain(|b| b.component != BatteryComponent::Case);
                            merged.push(case_info);
                            let merged_event =
                                AppEvent::AACPEvent(mac.clone(), Box::new(AE::BatteryInfo(merged)));
                            snapshot.retain(|e| !matches!(e, AppEvent::AACPEvent(m, ae) if m == mac && matches!(**ae, AE::BatteryInfo(_))));
                            snapshot.push(merged_event);
                            return;
                        }
                    }
                    snapshot.retain(|e| !matches!(e, AppEvent::AACPEvent(m, ae) if m == mac && matches!(**ae, AE::BatteryInfo(_))));
                }
                AE::ControlCommand(cmd) => {
                    let id = cmd.identifier;
                    snapshot.retain(|e| {
                        !matches!(e, AppEvent::AACPEvent(m, ae) if m == mac && matches!(&**ae, AE::ControlCommand(c) if c.identifier == id))
                    });
                }
                AE::DeviceInfo(_) => {
                    snapshot.retain(|e| !matches!(e, AppEvent::AACPEvent(m, ae) if m == mac && matches!(**ae, AE::DeviceInfo(_))));
                }
                AE::EarDetection { .. } => {
                    snapshot.retain(|e| !matches!(e, AppEvent::AACPEvent(m, ae) if m == mac && matches!(**ae, AE::EarDetection { .. })));
                }
                AE::SinglePod(_) => {
                    snapshot.retain(|e| !matches!(e, AppEvent::AACPEvent(m, ae) if m == mac && matches!(**ae, AE::SinglePod(_))));
                }
                AE::CaseLid(_) => {
                    snapshot.retain(|e| !matches!(e, AppEvent::AACPEvent(m, ae) if m == mac && matches!(**ae, AE::CaseLid(_))));
                }
                AE::ConnectedDevices(_, _) => {
                    snapshot.retain(|e| !matches!(e, AppEvent::AACPEvent(m, ae) if m == mac && matches!(**ae, AE::ConnectedDevices(_, _))));
                }
                // Transient events (StemPress, AudioSource, etc.) are not
                // meaningful to replay - skip storing them in the snapshot.
                _ => return,
            }
            snapshot.push(event.clone());
        }
        AppEvent::AudioUnavailable => {
            if !snapshot
                .iter()
                .any(|e| matches!(e, AppEvent::AudioUnavailable))
            {
                snapshot.push(event.clone());
            }
        }
    }
}

pub struct IpcServer {
    snapshot: StateSnapshot,
    broadcast_tx: broadcast::Sender<AppEvent>,
    cmd_tx: mpsc::UnboundedSender<(String, DeviceCommand)>,
    head_motion: Arc<MotionDemand>,
}

/// Clients asking for head motion. When their count crosses zero the
/// dispatcher gets `("*", HeadMotion(on))` to start or stop every session.
struct MotionDemand {
    clients: std::sync::Mutex<usize>,
    cmd_tx: mpsc::UnboundedSender<(String, DeviceCommand)>,
}

impl MotionDemand {
    /// Record whether one client asks; `asking` is that client's flag.
    fn set(&self, asking: &AtomicBool, on: bool) {
        let mut clients = self.clients.lock().unwrap_or_else(|e| e.into_inner());
        if asking.swap(on, Ordering::SeqCst) == on {
            return;
        }
        let crossed_zero = if on {
            *clients += 1;
            *clients == 1
        } else {
            *clients -= 1;
            *clients == 0
        };
        if crossed_zero {
            info!("Head motion {}", if on { "requested" } else { "released" });
            let _ = self
                .cmd_tx
                .send(("*".into(), DeviceCommand::HeadMotion(on)));
        }
    }
}

impl IpcServer {
    pub fn new(
        snapshot: StateSnapshot,
        cmd_tx: mpsc::UnboundedSender<(String, DeviceCommand)>,
    ) -> Self {
        let (broadcast_tx, _) = broadcast::channel(256);
        Self {
            snapshot,
            broadcast_tx,
            head_motion: Arc::new(MotionDemand {
                clients: std::sync::Mutex::new(0),
                cmd_tx: cmd_tx.clone(),
            }),
            cmd_tx,
        }
    }

    /// Broadcast an event to all connected clients.
    pub fn broadcast(&self, event: &AppEvent) {
        let _ = self.broadcast_tx.send(event.clone());
    }

    /// Run the IPC server, accepting connections on the Unix socket.
    pub async fn run(&self) -> std::io::Result<()> {
        self.serve(&socket_path()?).await
    }

    /// Accept connections on the Unix socket at `path`.
    pub async fn serve(&self, path: &Path) -> std::io::Result<()> {
        // Remove stale socket - ignore NotFound, log other errors
        if let Err(e) = std::fs::remove_file(path)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            log::warn!("Failed to remove stale socket {}: {}", path.display(), e);
        }

        let listener = UnixListener::bind(path)?;

        // Restrict socket to owner-only access
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Err(e) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            {
                log::warn!("Failed to set socket permissions: {}", e);
            }
        }

        info!("IPC server listening on {}", path.display());

        loop {
            let (stream, _) = listener.accept().await?;
            info!("IPC client connected");
            let snapshot = self.snapshot.clone();
            let mut event_rx = self.broadcast_tx.subscribe();
            let cmd_tx = self.cmd_tx.clone();
            let head_motion = self.head_motion.clone();
            // Whether this client asked for head motion.
            let asking = Arc::new(AtomicBool::new(false));

            tokio::spawn(async move {
                let (reader, writer) = stream.into_split();
                let mut reader = tokio::io::BufReader::new(reader);
                let mut writer = tokio::io::BufWriter::new(writer);

                // Replay snapshot
                {
                    let snap = snapshot.read().await;
                    for event in snap.iter() {
                        let json = match serde_json::to_vec(event) {
                            Ok(j) => j,
                            Err(e) => {
                                error!("Failed to serialize snapshot event: {}", e);
                                continue;
                            }
                        };
                        if write_msg(&mut writer, &json).await.is_err() {
                            return;
                        }
                    }
                }

                // Spawn writer task: forward broadcast events to client
                let (write_tx, mut write_rx) = mpsc::unbounded_channel::<Vec<u8>>();
                let writer_handle = tokio::spawn(async move {
                    while let Some(data) = write_rx.recv().await {
                        if write_msg(&mut writer, &data).await.is_err() {
                            break;
                        }
                    }
                });

                // Forward broadcast events
                let write_tx_clone = write_tx.clone();
                let forward_asking = asking.clone();
                let event_forward = tokio::spawn(async move {
                    loop {
                        match event_rx.recv().await {
                            Ok(event) => {
                                if event.is_head_motion() && !forward_asking.load(Ordering::SeqCst)
                                {
                                    continue;
                                }
                                if let Ok(json) = serde_json::to_vec(&event)
                                    && write_tx_clone.send(json).is_err()
                                {
                                    break;
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                info!("IPC client lagged by {} events", n);
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                });

                // Read commands from client
                while let Ok(data) = read_msg(&mut reader).await {
                    match serde_json::from_slice::<(String, DeviceCommand)>(&data) {
                        Ok((_, DeviceCommand::HeadMotion(on))) => head_motion.set(&asking, on),
                        Ok(cmd) => {
                            let _ = cmd_tx.send(cmd);
                        }
                        Err(e) => {
                            log::debug!("Skipping IPC command this daemon does not know: {}", e);
                        }
                    }
                }

                info!("IPC client disconnected");
                head_motion.set(&asking, false);
                event_forward.abort();
                writer_handle.abort();
            });
        }
    }
}

/// Connect to a running daemon via Unix socket.
/// Returns (cmd_tx, event_rx) that the TUI can use identically to in-process channels.
pub async fn ipc_connect() -> std::io::Result<(
    mpsc::UnboundedSender<(String, DeviceCommand)>,
    mpsc::UnboundedReceiver<AppEvent>,
)> {
    ipc_connect_to(&socket_path()?).await
}

/// Connect to a daemon listening at `path`.
pub async fn ipc_connect_to(
    path: &Path,
) -> std::io::Result<(
    mpsc::UnboundedSender<(String, DeviceCommand)>,
    mpsc::UnboundedReceiver<AppEvent>,
)> {
    let stream = UnixStream::connect(path).await?;
    info!("Connected to IPC daemon at {}", path.display());

    let (reader, writer) = stream.into_split();
    let mut reader = tokio::io::BufReader::new(reader);
    let mut writer = tokio::io::BufWriter::new(writer);

    let (event_tx, event_rx) = mpsc::unbounded_channel::<AppEvent>();
    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<(String, DeviceCommand)>();

    // Read events from daemon → event_tx
    tokio::spawn(async move {
        loop {
            match read_msg(&mut reader).await {
                Ok(data) => match serde_json::from_slice::<AppEvent>(&data) {
                    Ok(event) => {
                        if event_tx.send(event).is_err() {
                            break;
                        }
                    }
                    // Debug, not error: stderr would draw over the TUI.
                    Err(e) => {
                        log::debug!("Skipping IPC event this version does not know: {}", e);
                    }
                },
                Err(_) => {
                    info!("IPC connection closed");
                    break;
                }
            }
        }
    });

    // Write commands from cmd_tx → daemon
    tokio::spawn(async move {
        while let Some(cmd) = cmd_rx.recv().await {
            if let Ok(json) = serde_json::to_vec(&cmd)
                && write_msg(&mut writer, &json).await.is_err()
            {
                break;
            }
        }
    });

    Ok((cmd_tx, event_rx))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bluetooth::aacp::{
        AACPEvent as AE, AudioSource, AudioSourceType, BatteryComponent, BatteryInfo,
        BatteryStatus, ConnectedDevice, ControlCommandIdentifiers, ControlCommandStatus,
        EarDetectionStatus, HeadMotionEnd, HeadMotionSample, StemPressBudType, StemPressType,
    };
    use std::time::Duration;
    use tokio::sync::mpsc::UnboundedReceiver;
    use tokio::time::timeout;

    const MAC_A: &str = "AA:BB:CC:DD:EE:FF";
    const MAC_B: &str = "11:22:33:44:55:66";

    fn battery_event(mac: &str, infos: Vec<BatteryInfo>) -> AppEvent {
        AppEvent::AACPEvent(mac.into(), Box::new(AE::BatteryInfo(infos)))
    }

    fn control_event(mac: &str, id: ControlCommandIdentifiers, value: Vec<u8>) -> AppEvent {
        AppEvent::AACPEvent(
            mac.into(),
            Box::new(AE::ControlCommand(ControlCommandStatus {
                identifier: id,
                value,
            })),
        )
    }

    fn count_aacp(snap: &[AppEvent], mac: &str) -> usize {
        snap.iter()
            .filter(|e| matches!(e, AppEvent::AACPEvent(m, _) if m == mac))
            .count()
    }

    #[test]
    fn snapshot_replaces_device_on_reconnect() {
        let mut snap = Vec::new();
        let e1 = AppEvent::DeviceConnected {
            mac: MAC_A.into(),
            name: "Test".into(),
            product_id: 0x2014,
        };
        update_snapshot(&mut snap, &e1);
        assert_eq!(snap.len(), 1);

        let e2 = AppEvent::DeviceConnected {
            mac: MAC_A.into(),
            name: "Test Renamed".into(),
            product_id: 0x2014,
        };
        update_snapshot(&mut snap, &e2);
        assert_eq!(snap.len(), 1);
        match &snap[0] {
            AppEvent::DeviceConnected { name, .. } => assert_eq!(name, "Test Renamed"),
            _ => panic!("expected DeviceConnected"),
        }
    }

    #[test]
    fn snapshot_disconnect_removes_device() {
        let mut snap = Vec::new();
        update_snapshot(
            &mut snap,
            &AppEvent::DeviceConnected {
                mac: MAC_A.into(),
                name: "T".into(),
                product_id: 0,
            },
        );
        assert_eq!(snap.len(), 1);
        update_snapshot(&mut snap, &AppEvent::DeviceDisconnected(MAC_A.into()));
        assert!(snap.is_empty());
    }

    #[test]
    fn snapshot_disconnect_drops_only_target_device() {
        let mut snap = Vec::new();
        update_snapshot(
            &mut snap,
            &AppEvent::DeviceConnected {
                mac: MAC_A.into(),
                name: "A".into(),
                product_id: 0,
            },
        );
        update_snapshot(
            &mut snap,
            &AppEvent::DeviceConnected {
                mac: MAC_B.into(),
                name: "B".into(),
                product_id: 0,
            },
        );
        update_snapshot(&mut snap, &battery_event(MAC_B, vec![]));
        update_snapshot(&mut snap, &AppEvent::DeviceDisconnected(MAC_A.into()));
        // B's DeviceConnected + BatteryInfo survive
        assert_eq!(snap.len(), 2);
        assert!(matches!(&snap[0], AppEvent::DeviceConnected { mac, .. } if mac == MAC_B));
    }

    #[test]
    fn snapshot_battery_replaces_previous_battery() {
        let mut snap = Vec::new();
        update_snapshot(
            &mut snap,
            &battery_event(
                MAC_A,
                vec![BatteryInfo {
                    component: BatteryComponent::Left,
                    level: 50,
                    status: BatteryStatus::NotCharging,
                }],
            ),
        );
        update_snapshot(
            &mut snap,
            &battery_event(
                MAC_A,
                vec![BatteryInfo {
                    component: BatteryComponent::Left,
                    level: 60,
                    status: BatteryStatus::NotCharging,
                }],
            ),
        );
        assert_eq!(count_aacp(&snap, MAC_A), 1);
        match &snap[0] {
            AppEvent::AACPEvent(_, ae) => match &**ae {
                AE::BatteryInfo(b) => assert_eq!(b[0].level, 60),
                _ => panic!(),
            },
            _ => panic!(),
        }
    }

    #[test]
    fn snapshot_keeps_nearby_devices_until_a_session_or_disconnect() {
        let mut snap = Vec::new();
        let nearby = AppEvent::DeviceNearby {
            mac: MAC_A.into(),
            name: "Pods".into(),
            product_id: 0x2027,
        };
        update_snapshot(&mut snap, &nearby);
        update_snapshot(&mut snap, &nearby);
        assert_eq!(snap.len(), 1);
        update_snapshot(
            &mut snap,
            &AppEvent::DeviceConnected {
                mac: MAC_A.into(),
                name: "Pods".into(),
                product_id: 0x2027,
            },
        );
        assert!(
            !snap
                .iter()
                .any(|e| matches!(e, AppEvent::DeviceNearby { .. }))
        );
        // A broadcast does not reintroduce a connected device as nearby.
        update_snapshot(&mut snap, &nearby);
        assert!(
            !snap
                .iter()
                .any(|e| matches!(e, AppEvent::DeviceNearby { .. }))
        );
    }

    #[test]
    fn snapshot_preserves_case_when_new_event_has_disconnected_case() {
        let mut snap = Vec::new();
        update_snapshot(
            &mut snap,
            &battery_event(
                MAC_A,
                vec![
                    BatteryInfo {
                        component: BatteryComponent::Left,
                        level: 80,
                        status: BatteryStatus::NotCharging,
                    },
                    BatteryInfo {
                        component: BatteryComponent::Case,
                        level: 40,
                        status: BatteryStatus::NotCharging,
                    },
                ],
            ),
        );
        // case lid closed → reports Disconnected; should preserve previous case level
        update_snapshot(
            &mut snap,
            &battery_event(
                MAC_A,
                vec![
                    BatteryInfo {
                        component: BatteryComponent::Left,
                        level: 81,
                        status: BatteryStatus::NotCharging,
                    },
                    BatteryInfo {
                        component: BatteryComponent::Case,
                        level: 0,
                        status: BatteryStatus::Disconnected,
                    },
                ],
            ),
        );
        let merged = match &snap[0] {
            AppEvent::AACPEvent(_, ae) => match &**ae {
                AE::BatteryInfo(b) => b.clone(),
                _ => panic!(),
            },
            _ => panic!(),
        };
        let case = merged
            .iter()
            .find(|b| b.component == BatteryComponent::Case)
            .expect("case retained");
        assert_eq!(case.level, 40);
        assert_eq!(case.status, BatteryStatus::NotCharging);
    }

    #[test]
    fn snapshot_case_passthrough_when_present() {
        let mut snap = Vec::new();
        update_snapshot(
            &mut snap,
            &battery_event(
                MAC_A,
                vec![BatteryInfo {
                    component: BatteryComponent::Case,
                    level: 30,
                    status: BatteryStatus::NotCharging,
                }],
            ),
        );
        update_snapshot(
            &mut snap,
            &battery_event(
                MAC_A,
                vec![BatteryInfo {
                    component: BatteryComponent::Case,
                    level: 90,
                    status: BatteryStatus::Charging,
                }],
            ),
        );
        match &snap[0] {
            AppEvent::AACPEvent(_, ae) => match &**ae {
                AE::BatteryInfo(b) => {
                    let case = b
                        .iter()
                        .find(|x| x.component == BatteryComponent::Case)
                        .unwrap();
                    assert_eq!(case.level, 90);
                    assert_eq!(case.status, BatteryStatus::Charging);
                }
                _ => panic!(),
            },
            _ => panic!(),
        }
    }

    #[test]
    fn snapshot_control_command_replaces_per_identifier() {
        let mut snap = Vec::new();
        update_snapshot(
            &mut snap,
            &control_event(MAC_A, ControlCommandIdentifiers::ListeningMode, vec![0x02]),
        );
        update_snapshot(
            &mut snap,
            &control_event(MAC_A, ControlCommandIdentifiers::ListeningMode, vec![0x03]),
        );
        // different identifier should accumulate
        update_snapshot(
            &mut snap,
            &control_event(MAC_A, ControlCommandIdentifiers::OneBudAncMode, vec![0x01]),
        );
        assert_eq!(count_aacp(&snap, MAC_A), 2);
        let listening = snap.iter().find_map(|e| match e {
            AppEvent::AACPEvent(_, ae) => match &**ae {
                AE::ControlCommand(c)
                    if c.identifier == ControlCommandIdentifiers::ListeningMode =>
                {
                    Some(c.value.clone())
                }
                _ => None,
            },
            _ => None,
        });
        assert_eq!(listening, Some(vec![0x03]));
    }

    #[test]
    fn snapshot_keeps_devices_independent() {
        let mut snap = Vec::new();
        update_snapshot(
            &mut snap,
            &control_event(MAC_A, ControlCommandIdentifiers::ListeningMode, vec![0x02]),
        );
        update_snapshot(
            &mut snap,
            &control_event(MAC_B, ControlCommandIdentifiers::ListeningMode, vec![0x03]),
        );
        // Replacing A's listening mode must not touch B's
        update_snapshot(
            &mut snap,
            &control_event(MAC_A, ControlCommandIdentifiers::ListeningMode, vec![0x04]),
        );
        assert_eq!(count_aacp(&snap, MAC_A), 1);
        assert_eq!(count_aacp(&snap, MAC_B), 1);
    }

    #[test]
    fn snapshot_skips_transient_events() {
        let mut snap = Vec::new();
        // StemPress, AudioSource, ConversationalAwareness, OwnershipToFalse, ConnectionLost
        // are transient and should not appear in the replay snapshot.
        let stem = AppEvent::AACPEvent(
            MAC_A.into(),
            Box::new(AE::StemPress(
                StemPressType::Single,
                Some(StemPressBudType::Left),
            )),
        );
        let audio = AppEvent::AACPEvent(
            MAC_A.into(),
            Box::new(AE::AudioSource(AudioSource {
                mac: MAC_A.into(),
                r#type: AudioSourceType::Media,
            })),
        );
        let ca = AppEvent::AACPEvent(MAC_A.into(), Box::new(AE::ConversationalAwareness(1)));
        let lost = AppEvent::AACPEvent(MAC_A.into(), Box::new(AE::ConnectionLost));

        for e in [&stem, &audio, &ca, &lost] {
            update_snapshot(&mut snap, e);
        }
        assert!(
            snap.is_empty(),
            "transient events leaked into snapshot: {:?}",
            snap
        );
    }

    #[test]
    fn snapshot_replaces_ear_detection_per_device() {
        let mut snap = Vec::new();
        let mk = |l: EarDetectionStatus, r: EarDetectionStatus| {
            AppEvent::AACPEvent(
                MAC_A.into(),
                Box::new(AE::EarDetection {
                    old_left: None,
                    old_right: None,
                    new_left: Some(l),
                    new_right: Some(r),
                }),
            )
        };
        update_snapshot(
            &mut snap,
            &mk(EarDetectionStatus::OutOfEar, EarDetectionStatus::OutOfEar),
        );
        update_snapshot(
            &mut snap,
            &mk(EarDetectionStatus::InEar, EarDetectionStatus::InEar),
        );
        assert_eq!(count_aacp(&snap, MAC_A), 1);
        match &snap[0] {
            AppEvent::AACPEvent(_, ae) => match &**ae {
                AE::EarDetection {
                    new_left,
                    new_right,
                    ..
                } => {
                    assert_eq!(*new_left, Some(EarDetectionStatus::InEar));
                    assert_eq!(*new_right, Some(EarDetectionStatus::InEar));
                }
                _ => panic!(),
            },
            _ => panic!(),
        }
    }

    #[test]
    fn snapshot_replaces_connected_devices_per_device() {
        let mut snap = Vec::new();
        let mk = |peers: Vec<ConnectedDevice>| {
            AppEvent::AACPEvent(MAC_A.into(), Box::new(AE::ConnectedDevices(vec![], peers)))
        };
        update_snapshot(
            &mut snap,
            &mk(vec![ConnectedDevice {
                mac: "11:22:33:44:55:66".into(),
                info1: 0,
                info2: 0,
            }]),
        );
        update_snapshot(&mut snap, &mk(vec![]));
        assert_eq!(count_aacp(&snap, MAC_A), 1);
    }

    #[test]
    fn snapshot_audio_unavailable_dedupes() {
        let mut snap = Vec::new();
        update_snapshot(&mut snap, &AppEvent::AudioUnavailable);
        update_snapshot(&mut snap, &AppEvent::AudioUnavailable);
        update_snapshot(&mut snap, &AppEvent::AudioUnavailable);
        assert_eq!(
            snap.iter()
                .filter(|e| matches!(e, AppEvent::AudioUnavailable))
                .count(),
            1
        );
    }

    fn motion_event(mac: &str) -> AppEvent {
        AppEvent::AACPEvent(
            mac.into(),
            Box::new(AE::HeadMotion(HeadMotionSample {
                timestamp_ns: 123,
                quaternion: [1.0, 0.0, 0.0, 0.0],
            })),
        )
    }

    #[test]
    fn snapshot_never_stores_head_motion() {
        let mut snap = Vec::new();
        update_snapshot(&mut snap, &motion_event(MAC_A));
        update_snapshot(
            &mut snap,
            &AppEvent::AACPEvent(
                MAC_A.into(),
                Box::new(AE::HeadMotionEnded(HeadMotionEnd::OffHead)),
            ),
        );
        assert!(snap.is_empty(), "{snap:?}");
    }

    #[test]
    fn head_motion_has_the_agreed_wire_shape() {
        assert_eq!(
            serde_json::to_string(&motion_event("08:FF")).unwrap(),
            r#"{"AACPEvent":["08:FF",{"HeadMotion":{"timestamp_ns":123,"quaternion":[1.0,0.0,0.0,0.0]}}]}"#
        );
        let ended = AppEvent::AACPEvent(
            "08:FF".into(),
            Box::new(AE::HeadMotionEnded(HeadMotionEnd::OffHead)),
        );
        assert_eq!(
            serde_json::to_string(&ended).unwrap(),
            r#"{"AACPEvent":["08:FF",{"HeadMotionEnded":"OffHead"}]}"#
        );
        let cmd: (String, DeviceCommand) =
            serde_json::from_str(r#"["*",{"HeadMotion":true}]"#).unwrap();
        assert!(matches!(cmd.1, DeviceCommand::HeadMotion(true)));
    }

    /// The real server on a socket in its own runtime directory, with one
    /// device in the snapshot so a client knows it has been accepted once
    /// the replay arrives.
    async fn start_server(
        name: &str,
    ) -> (
        Arc<IpcServer>,
        UnboundedReceiver<(String, DeviceCommand)>,
        PathBuf,
    ) {
        let dir =
            std::env::temp_dir().join(format!("airpods-tui-ipc-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("airpods-tui.sock");
        let snapshot: StateSnapshot = Arc::new(RwLock::new(vec![AppEvent::DeviceConnected {
            mac: MAC_A.into(),
            name: "Max".into(),
            product_id: 0x200a,
        }]));
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let server = Arc::new(IpcServer::new(snapshot, cmd_tx));
        let serving = server.clone();
        let serve_path = path.clone();
        tokio::spawn(async move { serving.serve(&serve_path).await });
        (server, cmd_rx, path)
    }

    type Client = (
        mpsc::UnboundedSender<(String, DeviceCommand)>,
        UnboundedReceiver<AppEvent>,
    );

    async fn connect(path: &std::path::Path) -> Client {
        for _ in 0..200 {
            if let Ok((cmd_tx, mut events)) = ipc_connect_to(path).await {
                assert!(matches!(
                    next(&mut events).await,
                    Some(AppEvent::DeviceConnected { .. })
                ));
                return (cmd_tx, events);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("server never listened on {}", path.display());
    }

    async fn next<T>(rx: &mut UnboundedReceiver<T>) -> Option<T> {
        timeout(Duration::from_secs(2), rx.recv()).await.ok().flatten()
    }

    /// Nothing arrives within a short window.
    async fn quiet<T: std::fmt::Debug>(rx: &mut UnboundedReceiver<T>) {
        if let Ok(Some(x)) = timeout(Duration::from_millis(200), rx.recv()).await {
            panic!("unexpected {x:?}");
        }
    }

    fn ask(client: &Client, on: bool) {
        client
            .0
            .send(("*".into(), DeviceCommand::HeadMotion(on)))
            .unwrap();
    }

    fn assert_demand(cmd: Option<(String, DeviceCommand)>, on: bool) {
        assert!(
            matches!(&cmd, Some((_, DeviceCommand::HeadMotion(o))) if *o == on),
            "expected demand {on}, got {cmd:?}"
        );
    }

    #[tokio::test]
    async fn only_clients_that_ask_receive_head_motion() {
        let (server, mut demand, path) = start_server("forward").await;
        let asker = connect(&path).await;
        let mut other = connect(&path).await;
        ask(&asker, true);
        assert_demand(next(&mut demand).await, true);
        let mut asker = asker;

        let battery = battery_event(MAC_A, vec![]);
        server.broadcast(&motion_event(MAC_A));
        server.broadcast(&battery);
        assert!(next(&mut asker.1).await.unwrap().is_head_motion());
        assert!(!next(&mut asker.1).await.unwrap().is_head_motion());
        // The other client gets the battery report and nothing before it.
        assert!(matches!(
            next(&mut other.1).await,
            Some(AppEvent::AACPEvent(_, e)) if matches!(*e, AE::BatteryInfo(_))
        ));
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[tokio::test]
    async fn demand_lasts_until_the_last_asking_client_is_gone() {
        let (_server, mut demand, path) = start_server("demand").await;
        let first = connect(&path).await;
        let second = connect(&path).await;

        ask(&first, true);
        assert_demand(next(&mut demand).await, true);
        // Asking twice counts once; a second asker changes nothing.
        ask(&first, true);
        ask(&second, true);
        quiet(&mut demand).await;

        ask(&first, false);
        quiet(&mut demand).await;
        // The last asker disconnecting turns demand off.
        drop(second);
        assert_demand(next(&mut demand).await, false);
        quiet(&mut demand).await;
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }
}
