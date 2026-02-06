// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use super::clock::VirtualClock;
use super::network::VirtualNetwork;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::{Arc, Mutex};

pub type TaskId = u64;

struct ScheduledTask {
    id: TaskId,
    wake_at: u64,
    task: Box<dyn FnOnce() + Send>,
}

impl PartialEq for ScheduledTask {
    fn eq(&self, other: &Self) -> bool {
        self.wake_at == other.wake_at && self.id == other.id
    }
}

impl Eq for ScheduledTask {}

impl PartialOrd for ScheduledTask {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScheduledTask {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        Reverse(self.wake_at)
            .cmp(&Reverse(other.wake_at))
            .then_with(|| Reverse(self.id).cmp(&Reverse(other.id)))
    }
}

struct RuntimeState {
    tasks: BinaryHeap<ScheduledTask>,
    next_task_id: TaskId,
}

#[derive(Clone)]
pub struct SimulatedRuntime {
    clock: VirtualClock,
    network: VirtualNetwork,
    state: Arc<Mutex<RuntimeState>>,
}

impl SimulatedRuntime {
    #[must_use]
    pub fn new() -> Self {
        let clock = VirtualClock::new();
        let network = VirtualNetwork::new(clock.clone());
        Self {
            clock,
            network,
            state: Arc::new(Mutex::new(RuntimeState {
                tasks: BinaryHeap::new(),
                next_task_id: 0,
            })),
        }
    }

    #[must_use]
    pub fn clock(&self) -> &VirtualClock {
        &self.clock
    }

    #[must_use]
    pub fn network(&self) -> &VirtualNetwork {
        &self.network
    }

    pub fn schedule_at<F>(&self, wake_at: u64, task: F) -> TaskId
    where
        F: FnOnce() + Send + 'static,
    {
        let mut state = self.state.lock().unwrap();
        let id = state.next_task_id;
        state.next_task_id += 1;
        state.tasks.push(ScheduledTask {
            id,
            wake_at,
            task: Box::new(task),
        });
        id
    }

    pub fn schedule_after<F>(&self, delay_ms: u64, task: F) -> TaskId
    where
        F: FnOnce() + Send + 'static,
    {
        let wake_at = self.clock.now() + delay_ms * 1_000_000;
        self.schedule_at(wake_at, task)
    }

    pub fn run_until(&self, target_nanos: u64) -> usize {
        let mut executed = 0;

        loop {
            let task = {
                let mut state = self.state.lock().unwrap();
                match state.tasks.peek() {
                    Some(t) if t.wake_at <= target_nanos => state.tasks.pop(),
                    _ => None,
                }
            };

            match task {
                Some(t) => {
                    self.clock.set(t.wake_at);
                    (t.task)();
                    executed += 1;
                }
                None => break,
            }
        }

        self.clock.set(target_nanos);
        executed
    }

    pub fn run_for_ms(&self, ms: u64) -> usize {
        let target = self.clock.now() + ms * 1_000_000;
        self.run_until(target)
    }

    pub fn run_one(&self) -> bool {
        let task = {
            let mut state = self.state.lock().unwrap();
            state.tasks.pop()
        };

        match task {
            Some(t) => {
                self.clock.set(t.wake_at);
                (t.task)();
                true
            }
            None => false,
        }
    }

    pub fn pending_tasks(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.tasks.len()
    }

    #[allow(dead_code)]
    pub fn next_wake_time(&self) -> Option<u64> {
        let state = self.state.lock().unwrap();
        state.tasks.peek().map(|t| t.wake_at)
    }
}

impl Default for SimulatedRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn schedule_and_run() {
        let rt = SimulatedRuntime::new();
        let counter = Arc::new(AtomicU32::new(0));

        let c = counter.clone();
        rt.schedule_after(10, move || {
            c.fetch_add(1, Ordering::SeqCst);
        });

        assert_eq!(counter.load(Ordering::SeqCst), 0);
        rt.run_for_ms(5);
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        rt.run_for_ms(10);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn tasks_run_in_order() {
        let rt = SimulatedRuntime::new();
        let order = Arc::new(Mutex::new(Vec::new()));

        let o = order.clone();
        rt.schedule_after(30, move || o.lock().unwrap().push(3));

        let o = order.clone();
        rt.schedule_after(10, move || o.lock().unwrap().push(1));

        let o = order.clone();
        rt.schedule_after(20, move || o.lock().unwrap().push(2));

        rt.run_for_ms(50);

        assert_eq!(*order.lock().unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn clock_advances_to_task_time() {
        let rt = SimulatedRuntime::new();

        rt.schedule_after(100, || {});
        rt.run_one();

        assert_eq!(rt.clock().now(), 100_000_000);
    }

    #[test]
    fn run_until_stops_at_target() {
        let rt = SimulatedRuntime::new();
        let counter = Arc::new(AtomicU32::new(0));

        for i in 0..10 {
            let c = counter.clone();
            rt.schedule_after(i * 10, move || {
                c.fetch_add(1, Ordering::SeqCst);
            });
        }

        let executed = rt.run_until(50_000_000);
        assert_eq!(executed, 6);
        assert_eq!(counter.load(Ordering::SeqCst), 6);
    }

    #[test]
    fn pending_tasks_counts_correctly() {
        let rt = SimulatedRuntime::new();

        rt.schedule_after(10, || {});
        rt.schedule_after(20, || {});
        rt.schedule_after(30, || {});

        assert_eq!(rt.pending_tasks(), 3);
        rt.run_one();
        assert_eq!(rt.pending_tasks(), 2);
    }
}
