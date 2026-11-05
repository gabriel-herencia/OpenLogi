use std::sync::Arc;

use hidpp::channel::HidppChannel;

use super::{
    ArmedControl, HostSwitchError, HostSwitchRequest, HostSwitchRestoreOutcome, KeyboardSwitch,
    PendingHostSwitchRestore, ReportingMode, event_request, host_change_required, host_channel,
    prepare_host_change_on, restoration_change, rollback_host_switch_start, shares_channel,
    switch_linked_hosts_on,
};
use crate::backend::NodeId;
use crate::channel::scripted::{
    ScriptedBackend, ScriptedRawHidChannel, feature_error, scripted_channel as raw_scripted_channel,
};
use crate::reprog_controls::{
    AnalyticsKeyEvent, CidReporting, ControlId, CtrlIdInfo, ReprogControlsEvent,
};
use crate::{ChannelPool, ChannelRegistry, DeviceRoute, SharedChannel, device_io_channel};

/// Feature index the scripted keyboard reports for `0x1814 ChangeHost`.
const CHANGE_HOST_INDEX: u8 = 0x04;
/// Feature index the scripted keyboard reports for `0x1815 HostsInfo`.
const HOSTS_INFO_INDEX: u8 = 0x05;

/// `ErrorType::Busy`, the failure a scripted device answers with.
const BUSY: u8 = 0x08;
/// HID++ 1.0 `ErrorType::ResourceError`: the receiver's answer for a paired
/// device that is not connected to it.
const DISCONNECTED: u8 = 0x09;

/// What the scripted keyboard's firmware does when asked about `0x1815`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SlotStatus {
    /// Answers the query: hosts 0 and 1 paired, host 2 empty.
    Reported,
    /// Reports the feature as unimplemented, the usual index-0 lookup miss.
    Unimplemented,
    /// Errors on the lookup itself, as firmware that refuses unknown
    /// feature ids rather than reporting index 0 does.
    LookupErrors,
    /// Implements the feature but errors on the status read.
    ReadErrors,
}

/// A three-channel keyboard currently on host 0, paired on hosts 0 and 1
/// but **not** on host 2 — a keyboard with three host keys and only two
/// machines paired, which is the shape that used to strand devices.
fn keyboard_with_an_empty_third_slot(request: &[u8]) -> Option<Vec<u8>> {
    scripted_keyboard(request, SlotStatus::Reported)
}

/// The same keyboard without `0x1815`, so its slot pairing is unknowable.
fn keyboard_without_hosts_info(request: &[u8]) -> Option<Vec<u8>> {
    scripted_keyboard(request, SlotStatus::Unimplemented)
}

/// The same keyboard, whose firmware errors when asked for `0x1815`.
fn keyboard_erroring_on_hosts_info_lookup(request: &[u8]) -> Option<Vec<u8>> {
    scripted_keyboard(request, SlotStatus::LookupErrors)
}

/// The same keyboard, whose `0x1815` reads come back an error.
fn keyboard_erroring_on_slot_status(request: &[u8]) -> Option<Vec<u8>> {
    scripted_keyboard(request, SlotStatus::ReadErrors)
}

fn scripted_keyboard(request: &[u8], slot_status: SlotStatus) -> Option<Vec<u8>> {
    if request.len() < 7 || !matches!(request[0], 0x10 | 0x11) {
        return None;
    }
    let mut payload = [0u8; 16];
    match (request[2], request[3] >> 4) {
        // Root ping used by Device::new.
        (0x00, 0x01) => payload[0] = 4,
        // Root feature lookup.
        (0x00, 0x00) => {
            payload[0] = match u16::from_be_bytes([request[4], request[5]]) {
                0x1814 => CHANGE_HOST_INDEX,
                0x1815 => match slot_status {
                    SlotStatus::Unimplemented => 0x00,
                    SlotStatus::LookupErrors => return Some(feature_error(request, BUSY)),
                    SlotStatus::Reported | SlotStatus::ReadErrors => HOSTS_INFO_INDEX,
                },
                _ => 0x00,
            };
        }
        // ChangeHost getHostInfo: three RF channels, currently on host 0.
        (CHANGE_HOST_INDEX, 0x00) => payload[..2].copy_from_slice(&[3, 0]),
        // HostsInfo getHostInfo: echo the slot, then its pairing status.
        (HOSTS_INFO_INDEX, 0x01) => {
            if slot_status == SlotStatus::ReadErrors {
                return Some(feature_error(request, BUSY));
            }
            payload[0] = request[4];
            payload[1] = u8::from(request[4] < 2);
        }
        _ => return None,
    }

    let mut response = vec![0u8; 7];
    response[0] = 0x10;
    response[1..4].copy_from_slice(&request[1..4]);
    response[4..].copy_from_slice(&payload[..3]);
    Some(response)
}

async fn scripted_channel(responder: crate::channel::scripted::Responder) -> Arc<HidppChannel> {
    let (raw, _handle) = ScriptedRawHidChannel::with_responder(responder);
    crate::channel::scripted::scripted_channel(raw).await
}

#[tokio::test]
async fn switching_to_an_unpaired_slot_is_refused() {
    // ChangeHost would allow it: host 2 is within the device's channel
    // count. But nothing is paired there, and `setCurrentHost` is
    // fire-and-forget — the device would simply leave and not come back.
    let channel = scripted_channel(keyboard_with_an_empty_third_slot).await;

    let Err(error) = prepare_host_change_on(&channel, 1, 2).await else {
        panic!("an unpaired slot must not be switched to");
    };

    assert!(
        matches!(error, HostSwitchError::HostSlotEmpty { host: 2 }),
        "got {error:?}"
    );
}

#[tokio::test]
async fn switching_to_a_paired_slot_proceeds() {
    let channel = scripted_channel(keyboard_with_an_empty_third_slot).await;

    let change = prepare_host_change_on(&channel, 1, 1)
        .await
        .expect("a paired slot must be switchable");

    assert!(change.required, "host 1 differs from the current host 0");
}

#[tokio::test]
async fn a_device_without_hosts_info_is_still_switched() {
    // 0x1815 is the only source of per-slot pairing status. Without it the
    // guard must not block, or this change would regress every device that
    // does not implement it.
    let channel = scripted_channel(keyboard_without_hosts_info).await;

    let change = prepare_host_change_on(&channel, 1, 2)
        .await
        .expect("a device that cannot report slot status must still switch");

    assert!(change.required);
}

#[tokio::test]
async fn a_failed_hosts_info_lookup_does_not_block_the_switch() {
    // Firmware that answers an unknown feature id with an error rather than
    // index 0 must read the same as not implementing 0x1815 at all: the
    // pairing status is unknowable, which is not a reason to refuse.
    let channel = scripted_channel(keyboard_erroring_on_hosts_info_lookup).await;

    let change = prepare_host_change_on(&channel, 1, 2)
        .await
        .expect("an errored feature lookup must not abort the switch");

    assert!(change.required);
}

#[tokio::test]
async fn an_unreadable_slot_status_does_not_block_the_switch() {
    // The guard is advisory. A device that has 0x1815 but cannot answer for
    // it right now has not said the slot is empty, so refusing here would
    // turn a transient read failure into a dead host key.
    let channel = scripted_channel(keyboard_erroring_on_slot_status).await;

    let change = prepare_host_change_on(&channel, 1, 2)
        .await
        .expect("an errored status read must not abort the switch");

    assert!(change.required);
}

#[tokio::test]
async fn a_switch_to_the_current_host_never_consults_slot_status() {
    // Already-there is decided before the pairing check, so a device on an
    // unpaired-looking slot is not blocked from staying put.
    let channel = scripted_channel(keyboard_with_an_empty_third_slot).await;

    let change = prepare_host_change_on(&channel, 1, 0)
        .await
        .expect("staying on the current host is always fine");

    assert!(!change.required);
}

/// A reporting snapshot with unrelated bits deliberately set, so the
/// cleanup tests prove that only the bits they vary are restored.
fn noisy_reporting() -> CidReporting {
    CidReporting {
        cid: ControlId(0x00d3),
        diverted: false,
        persistently_diverted: true,
        force_raw_xy: true,
        raw_xy: false,
        remap: Some(ControlId(0x1234)),
        analytics_key_events: false,
        raw_wheel: true,
    }
}

fn direct_route() -> DeviceRoute {
    DeviceRoute::Direct {
        vendor_id: 0x046d,
        product_id: 0xb35b,
    }
}

fn armed_control() -> ArmedControl {
    ArmedControl {
        cid: 0x00d3,
        host: 0,
        mode: ReportingMode::Diverted,
        original: noisy_reporting(),
    }
}

#[tokio::test]
async fn pending_restore_recovers_only_through_a_new_current_publication() {
    let route = direct_route();
    let node = NodeId::from("keyboard-node".to_owned());
    let registry = ChannelRegistry::default();
    let (retired_raw, retired_handle) = ScriptedRawHidChannel::with_responder(|_| None);
    let retired_channel = raw_scripted_channel(retired_raw).await;
    let retired = SharedChannel::new(retired_channel.clone(), route.clone());
    registry.replace_node(node.clone(), [route.clone()], retired_channel);
    let pending = PendingHostSwitchRestore::new(&retired, 0x22, vec![armed_control()])
        .expect("one armed control must require restoration");

    let pending = match pending.retry(&registry).await {
        HostSwitchRestoreOutcome::RestorePending(pending) => pending,
        HostSwitchRestoreOutcome::Restored => {
            panic!("the retired publication must not restore itself")
        }
    };
    assert!(retired_handle.written_reports().is_empty());

    let (failed_raw, failed_handle) =
        ScriptedRawHidChannel::with_failing_writes(|request| Some(request.to_vec()), |_| true);
    let failed = raw_scripted_channel(failed_raw).await;
    registry.replace_node(node.clone(), [route.clone()], failed);
    let pending = match pending.retry(&registry).await {
        HostSwitchRestoreOutcome::RestorePending(pending) => pending,
        HostSwitchRestoreOutcome::Restored => panic!("failed writes cannot restore firmware"),
    };
    assert_eq!(failed_handle.written_reports().len(), 2);

    let (fresh_raw, fresh_handle) =
        ScriptedRawHidChannel::with_responder(|request| Some(request.to_vec()));
    registry.replace_node(node, [route], raw_scripted_channel(fresh_raw).await);

    assert!(matches!(
        pending.retry(&registry).await,
        HostSwitchRestoreOutcome::Restored
    ));
    assert_eq!(fresh_handle.written_reports().len(), 1);
}

#[tokio::test]
async fn suspended_partial_arm_rollback_returns_pending_without_writes() {
    let route = direct_route();
    let node = NodeId::from("keyboard-node".to_owned());
    let registry = ChannelRegistry::default();
    let (raw, handle) = ScriptedRawHidChannel::with_responder(|request| Some(request.to_vec()));
    let channel = raw_scripted_channel(raw).await;
    let shared = SharedChannel::new(channel.clone(), route.clone());
    registry.replace_node(node, [route], channel);
    let pending = PendingHostSwitchRestore::new(&shared, 0x22, vec![armed_control()]);
    let (signal, gate) = device_io_channel();
    assert!(signal.suspend());

    let failure = rollback_host_switch_start(
        HostSwitchError::Hidpp("partial arm failed".into()),
        pending,
        &registry,
        &gate,
    )
    .await;
    let (_, pending) = failure.into_parts();

    assert!(handle.written_reports().is_empty());
    assert!(signal.resume());
    assert!(matches!(
        pending
            .expect("failed rollback must retain ownership")
            .retry(&registry)
            .await,
        HostSwitchRestoreOutcome::Restored
    ));
    assert_eq!(handle.written_reports().len(), 1);
}

#[test]
fn receiver_slots_share_one_channel() {
    let keyboard = DeviceRoute::Bolt {
        receiver_uid: "AABB".into(),
        slot: 1,
    };
    let mouse = DeviceRoute::Bolt {
        receiver_uid: "aabb".into(),
        slot: 2,
    };
    assert!(shares_channel(&keyboard, &mouse));
}

#[test]
fn direct_devices_do_not_share_channels() {
    let route = DeviceRoute::Direct {
        vendor_id: 0x046d,
        product_id: 0xb025,
    };
    assert!(!shares_channel(&route, &route));
}

#[test]
fn host_controls_are_recognized_by_task_when_cid_varies() {
    let info = CtrlIdInfo {
        cid: 0x1234,
        task_id: 0x00af,
        flags: 0,
    };
    assert_eq!(host_channel(info), Some(1));
}

#[test]
fn analytics_event_selects_the_matching_host_already_switched_by_firmware() {
    let controls = [ArmedControl {
        cid: 0x00d3,
        host: 2,
        mode: ReportingMode::Analytics,
        original: noisy_reporting(),
    }];
    let mut events = [AnalyticsKeyEvent::default(); 5];
    events[0] = AnalyticsKeyEvent {
        cid: ControlId(0x00d3),
        event: 1,
    };
    assert_eq!(
        event_request(&controls, ReprogControlsEvent::AnalyticsKeyEvents(events)),
        Some(HostSwitchRequest {
            host: 2,
            keyboard: KeyboardSwitch::Firmware,
        })
    );
}

#[test]
fn diverted_press_leaves_the_keyboard_switch_to_openlogi() {
    let controls = [ArmedControl {
        cid: 0x00d2,
        host: 1,
        mode: ReportingMode::Diverted,
        original: noisy_reporting(),
    }];
    assert_eq!(
        event_request(
            &controls,
            ReprogControlsEvent::DivertedButtons([
                ControlId(0x00d2),
                ControlId(0),
                ControlId(0),
                ControlId(0)
            ])
        ),
        Some(HostSwitchRequest {
            host: 1,
            keyboard: KeyboardSwitch::OpenLogi,
        })
    );
}

fn bolt_slot(slot: u8) -> DeviceRoute {
    DeviceRoute::Bolt {
        receiver_uid: "aabb".into(),
        slot,
    }
}

/// A receiver whose keyboard on slot 1 has already left for another host —
/// the receiver answers for it with HID++ 1.0 `ResourceError`, as it does for
/// a disconnected device — while the mouse on slot 2 still answers.
fn receiver_after_a_firmware_host_switch(request: &[u8]) -> Option<Vec<u8>> {
    if request.get(1) == Some(&1) {
        return Some(vec![0x10, 1, 0x8f, request[2], request[3], DISCONNECTED, 0]);
    }
    scripted_keyboard(request, SlotStatus::Reported)
}

/// Whether `report` is a `setCurrentHost(host)` addressed to `device_index`.
fn is_set_current_host(report: &[u8], device_index: u8, host: u8) -> bool {
    report[1] == device_index
        && report[2] == CHANGE_HOST_INDEX
        && report[3] >> 4 == 1
        && report[4] == host
}

#[tokio::test]
async fn targets_follow_a_keyboard_its_firmware_already_switched() {
    let (raw, handle) =
        ScriptedRawHidChannel::with_responder(receiver_after_a_firmware_host_switch);
    let channel = raw_scripted_channel(raw).await;
    let pool = ChannelPool::with_backend(ScriptedBackend::new(Vec::new()));

    let switched = switch_linked_hosts_on(
        &bolt_slot(1),
        Some(&channel),
        &[bolt_slot(2)],
        HostSwitchRequest {
            host: 1,
            keyboard: KeyboardSwitch::Firmware,
        },
        &pool,
    )
    .await
    .expect("a departed keyboard must not abort its targets' switch");

    assert!(switched);
    let written = handle.written_reports();
    assert!(
        written
            .iter()
            .any(|report| is_set_current_host(report, 2, 1)),
        "the mouse must be sent to the keyboard's new host"
    );
    assert!(
        written.iter().all(|report| report[1] != 1),
        "the departed keyboard must not be addressed"
    );
}

#[tokio::test]
async fn an_unreachable_keyboard_keeps_its_targets_when_openlogi_must_switch_it() {
    let (raw, handle) =
        ScriptedRawHidChannel::with_responder(receiver_after_a_firmware_host_switch);
    let channel = raw_scripted_channel(raw).await;
    let pool = ChannelPool::with_backend(ScriptedBackend::new(Vec::new()));

    switch_linked_hosts_on(
        &bolt_slot(1),
        Some(&channel),
        &[bolt_slot(2)],
        HostSwitchRequest {
            host: 1,
            keyboard: KeyboardSwitch::OpenLogi,
        },
        &pool,
    )
    .await
    .expect_err("a keyboard that cannot follow must not strand its targets");

    assert!(
        handle
            .written_reports()
            .iter()
            .all(|report| !is_set_current_host(report, 2, 1)),
        "no target may move before the keyboard's own switch is validated"
    );
}

#[test]
fn current_host_does_not_require_a_change() {
    assert!(matches!(host_change_required(1, 3, 1), Ok(false)));
}

#[test]
fn different_valid_host_requires_a_change() {
    assert!(matches!(host_change_required(0, 3, 2), Ok(true)));
}

#[test]
fn host_outside_device_range_is_rejected() {
    assert!(
        host_change_required(0, 2, 2).is_err(),
        "host 2 is outside a device that reports 2 hosts and must be rejected"
    );
}

#[test]
fn diverted_cleanup_restores_only_the_original_temporary_bits() {
    let change = restoration_change(ArmedControl {
        cid: 0x00d3,
        host: 2,
        mode: ReportingMode::Diverted,
        original: CidReporting {
            diverted: true,
            raw_xy: true,
            ..noisy_reporting()
        },
    });

    assert_eq!(change.diverted, Some(true));
    assert_eq!(change.raw_xy, Some(true));
    assert_eq!(change.analytics_key_events, None);
    assert_eq!(change.persistently_diverted, None);
    assert_eq!(change.remap, None);
}

#[test]
fn analytics_cleanup_restores_the_original_analytics_bit() {
    let change = restoration_change(ArmedControl {
        cid: 0x00d3,
        host: 2,
        mode: ReportingMode::Analytics,
        original: CidReporting {
            analytics_key_events: true,
            ..noisy_reporting()
        },
    });

    assert_eq!(change.analytics_key_events, Some(true));
    assert_eq!(change.diverted, None);
    assert_eq!(change.raw_xy, None);
}
