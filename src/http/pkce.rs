use std::collections::HashMap;
use std::time::Instant;

const TTL_SECS: u64 = 300;

pub struct PkceCache {
    entries: HashMap<String, (String, Instant)>,
}

impl PkceCache {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn insert(&mut self, state: String, code_verifier: String) {
        self.cleanup();
        self.entries.insert(state, (code_verifier, Instant::now()));
    }

    pub fn take(&mut self, state: &str) -> Option<String> {
        self.cleanup();
        self.entries.remove(state).map(|(verifier, _)| verifier)
    }

    fn cleanup(&mut self) {
        let cutoff = Instant::now().checked_sub(std::time::Duration::from_secs(TTL_SECS)).unwrap();
        self.entries.retain(|_, (_, created)| *created > cutoff);
    }
}
