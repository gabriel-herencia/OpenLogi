//! Keep configured keyboard → pointing-device host-switch links armed.

use std::time::Duration;

use openlogi_hid::{
    ChannelPool, ChannelRegistry, DeviceIoGate, DeviceRoute, HostSwitchRequest,
    HostSwitchRestoreOutcome, HostSwitchStopReason, KeyboardSwitch, PendingHostSwitchRestore,
    run_host_switch_session, switch_linked_hosts,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;
use tracing::{debug, warn};

use super::retry::{RETRY_DELAY, wait_for_deadline};
use super::shutdown::{ManagerCompletion, WatcherHandle};
use crate::receiver_access::{ExclusiveAccessReason, ReceiverAccess, ReceiverRequestState};

const DEPARTURE_TIMEOUT: Duration = Duration::from_secs(10);

/// One resolved link. Config keys are converted to live routes by the
/// orchestrator so the transport watcher never needs to understand inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostSwitchLink {
    /// Keyboard whose host switch keys initiate the transition.
    pub keyboard: DeviceRoute,
    /// Pointing devices that follow the keyboard.
    pub targets: Vec<DeviceRoute>,
}

/// Read-only, lossless, coalescing view of resolved links.
pub type HostSwitchLinks = watch::Receiver<std::sync::Arc<Vec<HostSwitchLink>>>;

struct HostSwitchManagerContext {
    links: HostSwitchLinks,
    channel_pool: ChannelPool,
    registry: ChannelRegistry,
    receiver_access: ReceiverAccess,
    receiver_requests: watch::Receiver<ReceiverRequestState>,
    device_io: DeviceIoGate,
    shutdown: oneshot::Receiver<()>,
}

/// Spawn the host switch session manager.
#[must_use]
pub fn spawn(
    links: &HostSwitchLinks,
    channel_pool: ChannelPool,
    receiver_access: ReceiverAccess,
    registry: ChannelRegistry,
    device_io: DeviceIoGate,
) -> WatcherHandle {
    let links = links.clone();
    let receiver_requests = receiver_access.subscribe_requests();
    WatcherHandle::spawn("openlogi-host-switch-watcher", move |shutdown| {
        manage(HostSwitchManagerContext {
            links,
            channel_pool,
            registry,
            receiver_access,
            receiver_requests,
            device_io,
            shutdown,
        })
    })
}

/// Identity of one spawned host-switch session. A completion settles only the
/// slot carrying its epoch, so a stale task cannot settle its successor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SessionEpoch(u64);

impl SessionEpoch {
    fn next(self) -> Self {
        Self(self.0.wrapping_add(1))
    }
}

enum SessionPhase {
    Active(oneshot::Sender<HostSwitchStopReason>),
    Draining,
}

struct RunningSession {
    link: HostSwitchLink,
    epoch: SessionEpoch,
    phase: SessionPhase,
}

impl RunningSession {
    fn stop(&mut self, reason: HostSwitchStopReason) {
        let SessionPhase::Active(stop) = std::mem::replace(&mut self.phase, SessionPhase::Draining)
        else {
            return;
        };
        let _ = stop.send(reason);
    }
}

enum RestorePhase {
    Ready {
        token: PendingHostSwitchRestore,
        retry_at: Instant,
    },
    Restoring,
}

struct Recovery {
    link: HostSwitchLink,
    epoch: SessionEpoch,
    request: Option<HostSwitchRequest>,
    restore: RestorePhase,
}

enum HostSwitchSlot {
    Running(RunningSession),
    Recovering(Recovery),
    Restarting {
        link: HostSwitchLink,
        retry_at: Instant,
    },
}

impl HostSwitchSlot {
    fn keyboard(&self) -> &DeviceRoute {
        match self {
            Self::Running(session) => &session.link.keyboard,
            Self::Recovering(recovery) => &recovery.link.keyboard,
            Self::Restarting { link, .. } => &link.keyboard,
        }
    }
}

#[derive(Clone)]
struct TransitionIntent {
    link: HostSwitchLink,
    request: HostSwitchRequest,
}

impl TransitionIntent {
    /// Whether this press still asks for a transition.
    ///
    /// A keyboard its firmware already switched is leaving this host, and its
    /// link leaves inventory with it — the targets must follow regardless.
    fn is_current(&self, published: &[HostSwitchLink], terminal: bool) -> bool {
        !terminal
            && (self.request.keyboard == KeyboardSwitch::Firmware || published.contains(&self.link))
    }
}

enum TransitionPhase {
    Waiting(TransitionIntent),
    Running,
}

struct SessionCompletion {
    epoch: SessionEpoch,
    result: Result<SessionResult, tokio::task::JoinError>,
}

struct SessionResult {
    request: Option<HostSwitchRequest>,
    pending_restore: Option<PendingHostSwitchRestore>,
    failed: bool,
}

struct RestoreCompletion {
    epoch: SessionEpoch,
    result: Result<HostSwitchRestoreOutcome, tokio::task::JoinError>,
}

enum ManagerEvent {
    Session(SessionCompletion),
    Restore(RestoreCompletion),
    Transition(Result<(), tokio::task::JoinError>),
}

struct SessionServices {
    channel_pool: ChannelPool,
    registry: ChannelRegistry,
    receiver_access: ReceiverAccess,
    device_io: DeviceIoGate,
    events: mpsc::UnboundedSender<ManagerEvent>,
}

struct HostSwitchManagerState {
    slots: Vec<HostSwitchSlot>,
    last_epoch: SessionEpoch,
    transition: Option<TransitionPhase>,
    task_failed: bool,
}

impl HostSwitchManagerState {
    fn new() -> Self {
        Self {
            slots: Vec::new(),
            last_epoch: SessionEpoch(0),
            transition: None,
            task_failed: false,
        }
    }

    fn has_pending_restores(&self) -> bool {
        self.slots
            .iter()
            .any(|slot| matches!(slot, HostSwitchSlot::Recovering(_)))
    }

    fn has_running_sessions(&self) -> bool {
        self.slots
            .iter()
            .any(|slot| matches!(slot, HostSwitchSlot::Running(_)))
    }

    fn owns_keyboard(&self, keyboard: &DeviceRoute) -> bool {
        self.slots.iter().any(|slot| slot.keyboard() == keyboard)
    }

    fn reconcile_transition(&mut self, published: &[HostSwitchLink], terminal: bool) {
        if matches!(
            &self.transition,
            Some(TransitionPhase::Waiting(intent)) if !intent.is_current(published, terminal)
        ) {
            self.transition = None;
        }
    }

    fn begin_transition(&mut self, terminal: bool) -> Option<TransitionIntent> {
        if terminal || self.has_running_sessions() {
            return None;
        }
        // A restore owed to a keyboard its firmware moved away can only land
        // once it returns; its targets must not wait for that.
        let has_blocking_restores = self.has_pending_restores();
        let Some(TransitionPhase::Waiting(intent)) = self.transition.take_if(|phase| {
            matches!(
                phase,
                TransitionPhase::Waiting(intent)
                    if !has_blocking_restores
                        || intent.request.keyboard == KeyboardSwitch::Firmware
            )
        }) else {
            return None;
        };
        self.transition = Some(TransitionPhase::Running);
        Some(intent)
    }

    fn terminal_completion(&self, terminal: bool) -> Option<ManagerCompletion> {
        (terminal
            && !self.has_running_sessions()
            && !self.has_pending_restores()
            && self.transition.is_none())
        .then_some(if self.task_failed {
            ManagerCompletion::Unexpected
        } else {
            ManagerCompletion::Graceful
        })
    }

    fn deadline(&self, requests: ReceiverRequestState, device_io_allowed: bool) -> Option<Instant> {
        if requests.any() || !device_io_allowed {
            return None;
        }
        self.slots
            .iter()
            .filter_map(|slot| match slot {
                HostSwitchSlot::Recovering(Recovery {
                    restore: RestorePhase::Ready { retry_at, .. },
                    ..
                })
                | HostSwitchSlot::Restarting { retry_at, .. } => Some(*retry_at),
                HostSwitchSlot::Running(_) | HostSwitchSlot::Recovering(_) => None,
            })
            .min()
    }

    fn stop_sessions(&mut self, wanted: &[HostSwitchLink], terminal: bool) {
        for slot in &mut self.slots {
            let HostSwitchSlot::Running(session) = slot else {
                continue;
            };
            if terminal || !wanted.contains(&session.link) {
                session.stop(HostSwitchStopReason::Graceful);
            }
        }
    }

    fn reconcile_recoveries(
        &mut self,
        published: &[HostSwitchLink],
        requests: ReceiverRequestState,
        services: &SessionServices,
        terminal: bool,
    ) {
        let now = Instant::now();
        self.slots.retain(|slot| match slot {
            HostSwitchSlot::Restarting { link, retry_at } => {
                !terminal && published.contains(link) && (*retry_at > now || requests.any())
            }
            HostSwitchSlot::Running(_) | HostSwitchSlot::Recovering(_) => true,
        });
        for slot in &mut self.slots {
            let HostSwitchSlot::Recovering(recovery) = slot else {
                continue;
            };
            if !published.contains(&recovery.link) {
                recovery.request = None;
            }
            let RestorePhase::Ready { retry_at, .. } = &recovery.restore else {
                continue;
            };
            if *retry_at > now || requests.any() {
                continue;
            }
            let Some(lease) = services.receiver_access.try_acquire_for_session() else {
                break;
            };
            let epoch = recovery.epoch;
            let RestorePhase::Ready { token, .. } =
                std::mem::replace(&mut recovery.restore, RestorePhase::Restoring)
            else {
                continue;
            };
            let registry = services.registry.clone();
            let device_io = services.device_io.clone();
            let events = services.events.clone();
            tokio::spawn(async move {
                let task = tokio::spawn(async move {
                    let _lease = lease;
                    if device_io.allows_io() {
                        token.retry(&registry).await
                    } else {
                        HostSwitchRestoreOutcome::RestorePending(token)
                    }
                });
                let _ = events.send(ManagerEvent::Restore(RestoreCompletion {
                    epoch,
                    result: task.await,
                }));
            });
        }
    }

    fn spawn_successors(&mut self, wanted: &[HostSwitchLink], services: &SessionServices) {
        for link in wanted {
            if self.owns_keyboard(&link.keyboard) {
                continue;
            }
            let Some(lease) = services.receiver_access.try_acquire_for_session() else {
                break;
            };
            self.last_epoch = self.last_epoch.next();
            self.slots.push(HostSwitchSlot::Running(spawn_session(
                link.clone(),
                self.last_epoch,
                lease,
                services,
            )));
        }
    }

    fn handle_session_completion(
        &mut self,
        completion: SessionCompletion,
        published: &[HostSwitchLink],
        terminal: bool,
    ) {
        let Some(index) = self.slots.iter().position(|slot| {
            matches!(slot, HostSwitchSlot::Running(session) if session.epoch == completion.epoch)
        }) else {
            return;
        };
        let HostSwitchSlot::Running(session) = self.slots.remove(index) else {
            return;
        };
        let result = match completion.result {
            Ok(result) => result,
            Err(error) => {
                warn!(%error, route = %session.link.keyboard, "host switch session task failed");
                self.task_failed = true;
                return;
            }
        };
        if result.failed {
            debug!(route = %session.link.keyboard, "host switch session ended");
        }
        let link_is_current = !terminal && published.contains(&session.link);
        let intent = result
            .request
            .map(|request| TransitionIntent {
                link: session.link.clone(),
                request,
            })
            .filter(|intent| intent.is_current(published, terminal));
        match (result.pending_restore, intent) {
            // The firmware already moved the keyboard, so the restore it is
            // owed can only land once it returns. Its targets follow now.
            (Some(token), Some(intent)) if intent.request.keyboard == KeyboardSwitch::Firmware => {
                self.slots.push(HostSwitchSlot::Recovering(Recovery {
                    link: session.link,
                    epoch: session.epoch,
                    request: None,
                    restore: RestorePhase::Ready {
                        token,
                        retry_at: Instant::now() + RETRY_DELAY,
                    },
                }));
                self.transition = Some(TransitionPhase::Waiting(intent));
            }
            (Some(token), intent) => {
                self.slots.push(HostSwitchSlot::Recovering(Recovery {
                    link: session.link,
                    epoch: session.epoch,
                    request: intent.map(|intent| intent.request),
                    restore: RestorePhase::Ready {
                        token,
                        retry_at: Instant::now() + RETRY_DELAY,
                    },
                }));
            }
            (None, Some(intent)) => {
                self.transition = Some(TransitionPhase::Waiting(intent));
            }
            (None, None) if result.failed && link_is_current => {
                self.slots.push(HostSwitchSlot::Restarting {
                    link: session.link,
                    retry_at: Instant::now() + RETRY_DELAY,
                });
            }
            (None, None) => {}
        }
    }

    fn handle_restore_completion(
        &mut self,
        completion: RestoreCompletion,
        published: &[HostSwitchLink],
        terminal: bool,
    ) {
        let Some(index) = self.slots.iter().position(|slot| {
            matches!(slot, HostSwitchSlot::Recovering(recovery) if recovery.epoch == completion.epoch)
        }) else {
            return;
        };
        let HostSwitchSlot::Recovering(mut recovery) = self.slots.remove(index) else {
            return;
        };
        match completion.result {
            Ok(HostSwitchRestoreOutcome::RestorePending(token)) => {
                recovery.restore = RestorePhase::Ready {
                    token,
                    retry_at: Instant::now() + RETRY_DELAY,
                };
                self.slots.push(HostSwitchSlot::Recovering(recovery));
            }
            Ok(HostSwitchRestoreOutcome::Restored) => {
                let intent = recovery
                    .request
                    .map(|request| TransitionIntent {
                        link: recovery.link,
                        request,
                    })
                    .filter(|intent| intent.is_current(published, terminal));
                if let Some(intent) = intent {
                    self.transition = Some(TransitionPhase::Waiting(intent));
                }
            }
            Err(error) => {
                warn!(%error, route = %recovery.link.keyboard, "host switch restore task failed");
                self.task_failed = true;
            }
        }
    }
}

async fn manage(context: HostSwitchManagerContext) -> ManagerCompletion {
    let HostSwitchManagerContext {
        mut links,
        channel_pool,
        registry,
        receiver_access,
        mut receiver_requests,
        mut device_io,
        mut shutdown,
    } = context;
    let (events, mut event_rx) = mpsc::unbounded_channel();
    let mut registry_changes = registry.subscribe();
    let services = SessionServices {
        channel_pool,
        registry,
        receiver_access,
        device_io: device_io.clone(),
        events,
    };
    let mut state = HostSwitchManagerState::new();
    let mut terminal = false;

    loop {
        let requests = *receiver_requests.borrow_and_update();
        let published = std::sync::Arc::clone(&links.borrow_and_update());
        let io_allowed = device_io.allows_io();
        state.reconcile_transition(&published, terminal);
        let wanted = if terminal || requests.any() || state.transition.is_some() {
            &[][..]
        } else {
            published.as_slice()
        };
        if io_allowed || terminal {
            state.stop_sessions(wanted, terminal);
        }
        if io_allowed {
            state.reconcile_recoveries(&published, requests, &services, terminal);
            if !terminal && state.transition.is_none() {
                state.spawn_successors(wanted, &services);
            }
        }
        if let Some(completion) = state.terminal_completion(terminal) {
            return completion;
        }
        maybe_spawn_transition(&mut state, &links, &services, terminal);

        let deadline = state.deadline(*receiver_requests.borrow(), device_io.allows_io());
        if deadline.is_some_and(|deadline| deadline <= Instant::now()) {
            continue;
        }

        tokio::select! {
            biased;

            _ = &mut shutdown, if !terminal => {
                terminal = true;
            }
            Some(event) = event_rx.recv() => {
                let published = links.borrow().clone();
                handle_manager_event(&mut state, event, &published, terminal);
            }
            result = links.changed() => {
                if result.is_err() {
                    return ManagerCompletion::Unexpected;
                }
            }
            result = receiver_requests.changed() => {
                if result.is_err() {
                    return ManagerCompletion::Unexpected;
                }
            }
            allowed = device_io.changed() => match allowed {
                Some(_) => {}
                None => return ManagerCompletion::Unexpected,
            },
            changed = registry_changes.changed() => {
                if changed.is_err() {
                    return ManagerCompletion::Unexpected;
                }
                expedite_pending_restores(&mut state);
            }
            () = wait_for_deadline(deadline) => {}
        }
    }
}

fn handle_manager_event(
    state: &mut HostSwitchManagerState,
    event: ManagerEvent,
    published: &[HostSwitchLink],
    terminal: bool,
) {
    match event {
        ManagerEvent::Session(completion) => {
            state.handle_session_completion(completion, published, terminal);
        }
        ManagerEvent::Restore(completion) => {
            state.handle_restore_completion(completion, published, terminal);
        }
        ManagerEvent::Transition(result) => {
            if let Err(error) = result {
                warn!(%error, "host transition task failed");
                state.task_failed = true;
            }
            state.transition = None;
        }
    }
}

fn spawn_session(
    link: HostSwitchLink,
    epoch: SessionEpoch,
    receiver_lease: crate::receiver_access::SessionReceiverLease,
    services: &SessionServices,
) -> RunningSession {
    let (stop, stop_rx) = oneshot::channel();
    let session_link = link.clone();
    let registry = services.registry.clone();
    let device_io = services.device_io.clone();
    let events = services.events.clone();
    tokio::spawn(async move {
        let task = tokio::spawn(async move {
            let _receiver_lease = receiver_lease;
            match run_host_switch_session(
                session_link.keyboard.clone(),
                stop_rx,
                &registry,
                device_io,
            )
            .await
            {
                Ok(outcome) => {
                    let (request, pending_restore) = outcome.into_parts();
                    SessionResult {
                        request,
                        pending_restore,
                        failed: false,
                    }
                }
                Err(failure) => {
                    let (error, pending_restore) = failure.into_parts();
                    debug!(%error, route = %session_link.keyboard, "host switch session ended");
                    SessionResult {
                        request: None,
                        pending_restore,
                        failed: true,
                    }
                }
            }
        });
        let _ = events.send(ManagerEvent::Session(SessionCompletion {
            epoch,
            result: task.await,
        }));
    });
    RunningSession {
        link,
        epoch,
        phase: SessionPhase::Active(stop),
    }
}

fn maybe_spawn_transition(
    state: &mut HostSwitchManagerState,
    links: &HostSwitchLinks,
    services: &SessionServices,
    terminal: bool,
) {
    let Some(intent) = state.begin_transition(terminal) else {
        return;
    };
    let links = links.clone();
    let pool = services.channel_pool.clone();
    let receiver_access = services.receiver_access.clone();
    let device_io = services.device_io.clone();
    let events = services.events.clone();
    tokio::spawn(async move {
        let task = tokio::spawn(run_transition(
            links,
            pool,
            receiver_access,
            device_io,
            intent,
        ));
        let _ = events.send(ManagerEvent::Transition(task.await));
    });
}

async fn run_transition(
    mut links: HostSwitchLinks,
    channel_pool: ChannelPool,
    receiver_access: ReceiverAccess,
    device_io: DeviceIoGate,
    intent: TransitionIntent,
) {
    let _lease = receiver_access
        .acquire_exclusive(ExclusiveAccessReason::HostTransition)
        .await;
    if !device_io.allows_io() || !intent.is_current(&links.borrow(), false) {
        return;
    }
    match switch_linked_hosts(
        &intent.link.keyboard,
        &intent.link.targets,
        intent.request,
        &channel_pool,
    )
    .await
    {
        Ok(true) => wait_for_departure(&mut links, &intent.link.keyboard).await,
        Ok(false) => {}
        Err(error) => {
            debug!(%error, route = %intent.link.keyboard, host = intent.request.host, "keyboard host switch failed");
        }
    }
}

fn expedite_pending_restores(state: &mut HostSwitchManagerState) {
    let now = Instant::now();
    for slot in &mut state.slots {
        if let HostSwitchSlot::Recovering(Recovery {
            restore: RestorePhase::Ready { retry_at, .. },
            ..
        }) = slot
        {
            *retry_at = now;
        }
    }
}

async fn wait_for_departure(links: &mut HostSwitchLinks, keyboard: &DeviceRoute) {
    let deadline = tokio::time::sleep(DEPARTURE_TIMEOUT);
    tokio::pin!(deadline);
    loop {
        let departed = !links
            .borrow_and_update()
            .iter()
            .any(|link| link.keyboard == *keyboard);
        if departed {
            return;
        }
        tokio::select! {
            result = links.changed() => {
                if result.is_err() {
                    return;
                }
            }
            () = &mut deadline => {
                warn!(route = %keyboard, "host transition departure was not observed");
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(slot: u8) -> DeviceRoute {
        DeviceRoute::Bolt {
            receiver_uid: "cafe".to_owned(),
            slot,
        }
    }

    fn link(target: u8) -> HostSwitchLink {
        HostSwitchLink {
            keyboard: route(1),
            targets: vec![route(target)],
        }
    }

    #[test]
    fn target_change_drains_old_session_before_successor_can_arm() {
        let (stop, mut stop_rx) = oneshot::channel();
        let mut state = HostSwitchManagerState::new();
        state.slots.push(HostSwitchSlot::Running(RunningSession {
            link: link(2),
            epoch: SessionEpoch(1),
            phase: SessionPhase::Active(stop),
        }));

        state.stop_sessions(&[link(3)], false);

        assert_eq!(
            stop_rx
                .try_recv()
                .expect("old session should begin draining"),
            HostSwitchStopReason::Graceful,
        );
        assert!(state.slots.iter().any(|slot| slot.keyboard() == &route(1)));
    }

    fn intent(host: u8, keyboard: KeyboardSwitch) -> TransitionIntent {
        TransitionIntent {
            link: link(2),
            request: HostSwitchRequest { host, keyboard },
        }
    }

    #[test]
    fn stale_link_invalidates_transition_intent() {
        let mut state = HostSwitchManagerState::new();
        state.transition = Some(TransitionPhase::Waiting(intent(
            1,
            KeyboardSwitch::OpenLogi,
        )));

        state.reconcile_transition(&[link(3)], false);
        assert!(state.transition.is_none());
        assert!(state.begin_transition(false).is_none());
    }

    #[test]
    fn firmware_switched_keyboard_departure_keeps_its_targets_transition() {
        // The keyboard's own firmware moved it, so its link leaves inventory
        // with it; the targets must still follow.
        let mut state = HostSwitchManagerState::new();
        state.transition = Some(TransitionPhase::Waiting(intent(
            1,
            KeyboardSwitch::Firmware,
        )));

        state.reconcile_transition(&[], false);
        let started = state
            .begin_transition(false)
            .expect("a departed keyboard's targets must still be switched");
        assert_eq!(started.request.host, 1);
    }

    #[test]
    fn terminal_shutdown_drops_a_firmware_switched_transition() {
        let mut state = HostSwitchManagerState::new();
        state.transition = Some(TransitionPhase::Waiting(intent(
            1,
            KeyboardSwitch::Firmware,
        )));

        state.reconcile_transition(&[], true);
        assert!(state.transition.is_none());
    }

    #[test]
    fn firmware_switched_press_starts_transition_after_its_link_departs() {
        let mut state = HostSwitchManagerState::new();
        let (stop, _stop_rx) = oneshot::channel();
        state.slots.push(HostSwitchSlot::Running(RunningSession {
            link: link(2),
            epoch: SessionEpoch(1),
            phase: SessionPhase::Active(stop),
        }));
        let request = HostSwitchRequest {
            host: 1,
            keyboard: KeyboardSwitch::Firmware,
        };

        handle_manager_event(
            &mut state,
            ManagerEvent::Session(SessionCompletion {
                epoch: SessionEpoch(1),
                result: Ok(SessionResult {
                    request: Some(request),
                    pending_restore: None,
                    failed: false,
                }),
            }),
            &[],
            false,
        );

        let started = state
            .begin_transition(false)
            .expect("the press must still switch the targets");
        assert_eq!(started.request, request);
    }

    #[test]
    fn diverted_press_is_dropped_once_its_link_departs() {
        let mut state = HostSwitchManagerState::new();
        let (stop, _stop_rx) = oneshot::channel();
        state.slots.push(HostSwitchSlot::Running(RunningSession {
            link: link(2),
            epoch: SessionEpoch(1),
            phase: SessionPhase::Active(stop),
        }));

        handle_manager_event(
            &mut state,
            ManagerEvent::Session(SessionCompletion {
                epoch: SessionEpoch(1),
                result: Ok(SessionResult {
                    request: Some(HostSwitchRequest {
                        host: 1,
                        keyboard: KeyboardSwitch::OpenLogi,
                    }),
                    pending_restore: None,
                    failed: false,
                }),
            }),
            &[],
            false,
        );

        assert!(state.begin_transition(false).is_none());
    }

    #[test]
    fn firmware_switched_transition_does_not_wait_for_the_departed_keyboards_restore() {
        let mut state = HostSwitchManagerState::new();
        state.slots.push(HostSwitchSlot::Recovering(Recovery {
            link: link(2),
            epoch: SessionEpoch(1),
            request: None,
            restore: RestorePhase::Restoring,
        }));
        state.transition = Some(TransitionPhase::Waiting(intent(
            1,
            KeyboardSwitch::Firmware,
        )));

        assert!(
            state.begin_transition(false).is_some(),
            "the restore can only land once the keyboard returns"
        );
    }

    #[test]
    fn restoring_firmware_blocks_a_changed_link_for_the_same_keyboard() {
        let mut state = HostSwitchManagerState::new();
        state.slots.push(HostSwitchSlot::Recovering(Recovery {
            link: link(2),
            epoch: SessionEpoch(1),
            request: None,
            restore: RestorePhase::Restoring,
        }));

        assert!(
            state.owns_keyboard(&link(3).keyboard),
            "target changes must not permit re-arm over pending keyboard firmware"
        );
    }

    #[test]
    fn terminal_completion_waits_for_restore_acknowledgement() {
        let mut state = HostSwitchManagerState::new();
        state.slots.push(HostSwitchSlot::Recovering(Recovery {
            link: link(2),
            epoch: SessionEpoch(1),
            request: None,
            restore: RestorePhase::Restoring,
        }));

        assert!(state.terminal_completion(true).is_none());
        state.handle_restore_completion(
            RestoreCompletion {
                epoch: SessionEpoch(0),
                result: Ok(HostSwitchRestoreOutcome::Restored),
            },
            &[],
            true,
        );
        assert!(
            state.terminal_completion(true).is_none(),
            "stale completion cannot discard recovery"
        );
        state.handle_restore_completion(
            RestoreCompletion {
                epoch: SessionEpoch(1),
                result: Ok(HostSwitchRestoreOutcome::Restored),
            },
            &[],
            true,
        );
        assert!(matches!(
            state.terminal_completion(true),
            Some(ManagerCompletion::Graceful)
        ));
    }

    #[test]
    fn transition_waits_for_restoration_and_keeps_running_until_acknowledged() {
        let mut state = HostSwitchManagerState::new();
        state.slots.push(HostSwitchSlot::Recovering(Recovery {
            link: link(2),
            epoch: SessionEpoch(1),
            request: None,
            restore: RestorePhase::Restoring,
        }));
        state.transition = Some(TransitionPhase::Waiting(intent(
            2,
            KeyboardSwitch::OpenLogi,
        )));
        assert!(state.begin_transition(false).is_none());
        assert!(matches!(
            state.transition,
            Some(TransitionPhase::Waiting(_))
        ));

        state.handle_restore_completion(
            RestoreCompletion {
                epoch: SessionEpoch(1),
                result: Ok(HostSwitchRestoreOutcome::Restored),
            },
            &[link(2)],
            false,
        );
        assert_eq!(state.begin_transition(false).unwrap().request.host, 2);
        // Another manager wake while switching must not remove Running.
        assert!(state.begin_transition(false).is_none());
        assert!(state.terminal_completion(true).is_none());
        handle_manager_event(&mut state, ManagerEvent::Transition(Ok(())), &[], true);
        assert!(matches!(
            state.terminal_completion(true),
            Some(ManagerCompletion::Graceful)
        ));
    }

    #[tokio::test]
    async fn completed_session_releases_receiver_lease_before_manager_acknowledgement() {
        let access = ReceiverAccess::default();
        let registry = ChannelRegistry::default();
        let (_signal, gate) = openlogi_hid::device_io_channel();
        let (events, mut received) = mpsc::unbounded_channel();
        let services = SessionServices {
            channel_pool: openlogi_hid::channel_pool(),
            registry,
            receiver_access: access.clone(),
            device_io: gate,
            events,
        };
        let _session = spawn_session(
            link(2),
            SessionEpoch(1),
            access.try_acquire_for_session().unwrap(),
            &services,
        );
        let _exclusive = tokio::time::timeout(
            Duration::from_secs(1),
            access.acquire_exclusive(ExclusiveAccessReason::Pairing),
        )
        .await
        .expect("the failed session must release its lease even before the manager consumes Done");
        let Some(ManagerEvent::Session(completion)) = received.recv().await else {
            panic!("expected session completion");
        };
        assert!(completion.result.unwrap().failed);
    }

    #[test]
    fn suspended_device_io_disables_retry_deadlines() {
        let retry_at = Instant::now() + RETRY_DELAY;
        let mut state = HostSwitchManagerState::new();
        state.slots.push(HostSwitchSlot::Restarting {
            link: link(2),
            retry_at,
        });

        assert_eq!(
            state.deadline(ReceiverRequestState::default(), true),
            Some(retry_at)
        );
        assert_eq!(state.deadline(ReceiverRequestState::default(), false), None);
    }

    #[tokio::test(start_paused = true)]
    async fn departure_publication_finishes_wait_without_advancing_time() {
        let keyboard = route(1);
        let active = HostSwitchLink {
            keyboard: keyboard.clone(),
            targets: vec![route(2)],
        };
        let (links, mut published) = watch::channel(std::sync::Arc::new(vec![active]));
        let started = Instant::now();
        let waiting = tokio::spawn(async move {
            wait_for_departure(&mut published, &keyboard).await;
            Instant::now()
        });
        tokio::task::yield_now().await;

        links.send_replace(std::sync::Arc::new(Vec::new()));
        tokio::task::yield_now().await;

        assert_eq!(
            waiting.await.expect("departure waiter should finish"),
            started,
            "the link publication should reconcile departure immediately"
        );
    }
}
