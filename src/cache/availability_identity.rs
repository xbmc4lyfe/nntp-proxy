//! Stable article-availability identities, separate from transport endpoints.

use crate::config::Server;
use crate::types::{BackendId, HostName};
use anyhow::{Context, Result};
use std::fmt;
use std::fs;
use std::hash::Hasher;
use std::num::NonZeroU64;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use twox_hash::XxHash64;

const REGISTRY_MAGIC: &[u8; 8] = b"ANEGREG2";
const MAX_REGISTRY_FIELD_BYTES: usize = 1024 * 1024;

/// A registry generation cannot use the legacy/unscoped zero value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RegistryEpoch(NonZeroU64);

impl RegistryEpoch {
    /// A random new generation, never equal to `previous`.
    fn fresh(previous: Option<Self>) -> Self {
        loop {
            let epoch = Self(
                NonZeroU64::new(uuid::Uuid::new_v4().as_u128() as u64).unwrap_or(NonZeroU64::MIN),
            );
            if Some(epoch) != previous {
                return epoch;
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LayoutEpoch {
    Derived,
    Registered(RegistryEpoch),
}

/// A configured backend host whose authoritative article facts may be shared.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct AvailabilityIdentity(String);

impl From<&HostName> for AvailabilityIdentity {
    fn from(host: &HostName) -> Self {
        Self(canonicalize_host(host.as_ref()))
    }
}

impl AvailabilityIdentity {
    /// The identity whose `430`s a server shares: its backbone when one is
    /// configured, otherwise its host. Backbone identities carry a prefix
    /// with `=`, which no hostname contains, so a backbone can never collide
    /// with a host of the same spelling.
    pub(crate) fn for_server(server: &Server) -> Self {
        match server.backbone.as_deref().map(str::trim) {
            // Canonicalized as a persisted identity will be, so a reload
            // finds the same slot.
            Some(backbone) if !backbone.is_empty() && !is_auto(backbone) => {
                Self::backbone(backbone)
            }
            _ => Self::from(&server.host),
        }
    }

    /// The identity of a named backbone, spelled as [`Self::for_server`]
    /// spells a configured one.
    pub(crate) fn backbone(name: &str) -> Self {
        Self(canonicalize_host(&format!(
            "backbone={}",
            name.trim().to_ascii_lowercase()
        )))
    }

    /// Persistence preserves the configured hostname identity without DNS
    /// resolution, applying hostname case and trailing-dot canonicalization.
    pub(crate) fn from_persisted_host(host: String) -> Result<Self> {
        let host = canonicalize_host(&host);
        if host.is_empty() || host.len() > MAX_REGISTRY_FIELD_BYTES {
            anyhow::bail!("invalid availability host length");
        }
        Ok(Self(host))
    }

    pub(crate) fn as_host(&self) -> &str {
        &self.0
    }
}

/// `backbone = "auto"` asks for detection, the same as leaving it out.
pub(crate) fn is_auto(backbone: &str) -> bool {
    backbone.trim().eq_ignore_ascii_case("auto")
}

fn canonicalize_host(host: &str) -> String {
    host.strip_suffix('.').unwrap_or(host).to_ascii_lowercase()
}

/// Bit position used by article availability state.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AvailabilitySlot(usize);

impl AvailabilitySlot {
    #[must_use]
    pub const fn new(index: usize) -> Option<Self> {
        if index < usize::BITS as usize {
            Some(Self(index))
        } else {
            None
        }
    }

    #[must_use]
    pub(crate) const fn bit(self) -> usize {
        1usize << self.0
    }

    #[must_use]
    pub(crate) const fn index(self) -> usize {
        self.0
    }
}

/// Set of configured provider slots used for exhaustion decisions.
///
/// Slots are provider identities, not transport backend positions. A router
/// can therefore have duplicate backend entries for one slot and sparse slots
/// when identities are restored from the registry.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct AvailabilityMask(usize);

impl AvailabilityMask {
    #[must_use]
    pub const fn empty() -> Self {
        Self(0)
    }

    pub fn insert(&mut self, slot: AvailabilitySlot) {
        self.0 |= slot.bit();
    }

    #[must_use]
    pub fn from_slots(slots: &[AvailabilitySlot]) -> Self {
        let mut mask = Self::empty();
        for &slot in slots {
            mask.insert(slot);
        }
        mask
    }

    #[must_use]
    pub const fn bits(self) -> usize {
        self.0
    }
}

impl fmt::Display for AvailabilitySlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Which slot each backend records its `430`s under.
///
/// One table is shared by the layout, the router and the caches, so moving a
/// backend to another backbone reaches every reader at once without a
/// restart. A reader may act on the old slot for a request already in flight;
/// that only records or skips under the identity the backend had a moment
/// ago, which was true when the request started.
#[derive(Debug)]
pub(crate) struct BackendSlots {
    slots: [AtomicU8; usize::BITS as usize],
    len: AtomicUsize,
    mask: AtomicUsize,
    /// Slots more than one backend records under.
    shared: AtomicUsize,
    /// Serializes writers, so the mask is always recomputed from every
    /// slot store before it; readers stay lock-free.
    writer: std::sync::Mutex<()>,
}

impl BackendSlots {
    #[must_use]
    pub(crate) fn new(slots: &[AvailabilitySlot]) -> Self {
        let table = Self {
            slots: std::array::from_fn(|_| AtomicU8::new(0)),
            len: AtomicUsize::new(0),
            mask: AtomicUsize::new(0),
            shared: AtomicUsize::new(0),
            writer: std::sync::Mutex::new(()),
        };
        for (index, slot) in slots.iter().enumerate() {
            table.set(BackendId::from_index(index), *slot);
        }
        table
    }

    #[must_use]
    pub(crate) fn get(&self, backend: BackendId) -> AvailabilitySlot {
        AvailabilitySlot(usize::from(
            self.slots[backend.as_index()].load(Ordering::Relaxed),
        ))
    }

    /// Slots any backend records under, for deciding when all are exhausted.
    #[must_use]
    pub(crate) fn mask(&self) -> AvailabilityMask {
        AvailabilityMask(self.mask.load(Ordering::Relaxed))
    }

    /// Whether more than one backend records under `slot`.
    #[must_use]
    pub(crate) fn is_shared(&self, slot: AvailabilitySlot) -> bool {
        self.shared.load(Ordering::Relaxed) & slot.bit() != 0
    }

    /// Backends that record under `slot` right now.
    pub(crate) fn backends_on(&self, slot: AvailabilitySlot) -> Vec<BackendId> {
        (0..self.len.load(Ordering::Relaxed))
            .map(BackendId::from_index)
            .filter(|backend| self.get(*backend) == slot)
            .collect()
    }

    pub(crate) fn set(&self, backend: BackendId, slot: AvailabilitySlot) {
        let _writer = self
            .writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let index = backend.as_index();
        let slot = u8::try_from(slot.index()).expect("slots fit the bitmap width");
        self.slots[index].store(slot, Ordering::Relaxed);
        self.len.fetch_max(index + 1, Ordering::Relaxed);
        let len = self.len.load(Ordering::Relaxed);
        let (mask, shared) =
            self.slots[..len]
                .iter()
                .fold((0usize, 0usize), |(mask, shared), slot| {
                    let bit = 1usize << slot.load(Ordering::Relaxed);
                    (mask | bit, shared | (mask & bit))
                });
        self.mask.store(mask, Ordering::Relaxed);
        self.shared.store(shared, Ordering::Relaxed);
    }
}

/// Mapping from configured backend entries to provider identities.
///
/// The identity list is fixed for the process; the backend-to-slot table can
/// change at runtime (see [`Self::reassign`]). Clones share that table.
#[derive(Clone, Debug)]
pub(crate) struct AvailabilityLayout {
    backend_slots: Arc<BackendSlots>,
    miss_sampler: Arc<super::miss_sampler::MissSampler>,
    identities: Box<[AvailabilityIdentity]>,
    epoch: LayoutEpoch,
}

impl PartialEq for AvailabilityLayout {
    fn eq(&self, other: &Self) -> bool {
        self.identities == other.identities
            && self.epoch == other.epoch
            && self.backend_slots.len.load(Ordering::Relaxed)
                == other.backend_slots.len.load(Ordering::Relaxed)
            && (0..self.backend_slots.len.load(Ordering::Relaxed)).all(|index| {
                let backend = BackendId::from_index(index);
                self.backend_slots.get(backend) == other.backend_slots.get(backend)
            })
    }
}

impl Eq for AvailabilityLayout {}

/// The configured identities first, in server order, then `extra` ones a
/// backend may move to later, without duplicates.
fn identities_with_candidates(
    servers: &[Server],
    extra: &[AvailabilityIdentity],
) -> Vec<AvailabilityIdentity> {
    let mut identities = Vec::new();
    let configured = servers.iter().map(AvailabilityIdentity::for_server);
    // Every host stays registered so a backend can always fall back to
    // sharing only with its own host.
    let hosts = servers
        .iter()
        .map(|server| AvailabilityIdentity::from(&server.host));
    for identity in configured.chain(hosts).chain(extra.iter().cloned()) {
        if !identities.contains(&identity) {
            identities.push(identity);
        }
    }
    identities
}

impl AvailabilityLayout {
    #[must_use]
    pub(crate) fn synthetic(count: usize) -> Self {
        let identities = (0..count)
            .map(|index| AvailabilityIdentity(format!("backend-{index}")))
            .collect::<Vec<_>>();
        let backend_slots = (0..count)
            .map(|index| AvailabilitySlot::new(index).expect("synthetic count fits bitmap"))
            .collect::<Vec<_>>();
        let slots = Arc::new(BackendSlots::new(&backend_slots));
        Self {
            miss_sampler: Arc::new(super::miss_sampler::MissSampler::new(Arc::clone(&slots))),
            backend_slots: slots,
            identities: identities.into_boxed_slice(),
            epoch: LayoutEpoch::Derived,
        }
    }

    /// Build the compact current-process layout, deduplicating configured hosts.
    #[cfg(test)]
    pub(crate) fn from_servers(servers: &[Server]) -> Result<Self, AvailabilityLayoutError> {
        Self::from_servers_with_candidates(servers, &[])
    }

    /// As [`Self::from_servers`], also registering `extra` identities (known
    /// backbones) that backbone detection may move a backend to later.
    pub(crate) fn from_servers_with_candidates(
        servers: &[Server],
        extra: &[AvailabilityIdentity],
    ) -> Result<Self, AvailabilityLayoutError> {
        let mut identities = identities_with_candidates(servers, extra);
        // Candidates beyond the bitmap width are dropped. The configured
        // identities come first, so they always stay; with more than 32
        // servers some hosts may not, and moving such a server back to its
        // own host is refused with a warning.
        identities.truncate(usize::BITS as usize);
        let mut backend_slots = Vec::with_capacity(servers.len());
        for server in servers {
            let identity = AvailabilityIdentity::for_server(server);
            let slot = identities
                .iter()
                .position(|existing| existing == &identity)
                .expect("identity was collected above");
            backend_slots.push(
                AvailabilitySlot::new(slot).ok_or(AvailabilityLayoutError::TooManyIdentities)?,
            );
        }

        let slots = Arc::new(BackendSlots::new(&backend_slots));
        Ok(Self {
            miss_sampler: Arc::new(super::miss_sampler::MissSampler::new(Arc::clone(&slots))),
            backend_slots: slots,
            identities: identities.into_boxed_slice(),
            epoch: LayoutEpoch::Derived,
        })
    }

    /// Load or atomically publish the hybrid registry before cache startup.
    #[cfg(test)]
    pub(crate) fn from_hybrid_registry(servers: &[Server], cache_path: &Path) -> Result<Self> {
        Self::from_hybrid_registry_with_candidates(servers, cache_path, &[])
    }

    /// As [`Self::from_hybrid_registry`], also registering `extra` identities
    /// that backbone detection may move a backend to later.
    pub(crate) fn from_hybrid_registry_with_candidates(
        servers: &[Server],
        cache_path: &Path,
        extra: &[AvailabilityIdentity],
    ) -> Result<Self> {
        fs::create_dir_all(cache_path)
            .with_context(|| format!("create hybrid cache directory {}", cache_path.display()))?;
        let path = cache_path.join("availability.registry");
        let loaded = match fs::read(&path) {
            Ok(data) => parse_registry(&data).ok(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error).with_context(|| format!("read {}", path.display())),
        };
        let mut changed = loaded.is_none();
        let (mut identities, mut epoch) =
            loaded.unwrap_or_else(|| (Vec::new(), RegistryEpoch::fresh(None)));
        let mut configured = identities_with_candidates(servers, extra);
        configured.truncate(usize::BITS as usize);
        let unregistered = configured
            .iter()
            .filter(|identity| !identities.contains(identity))
            .count();
        if identities.len() + unregistered > usize::BITS as usize {
            // Slots are never reused within an epoch, so a registry that has
            // seen a slot's worth of hosts over its lifetime has none left.
            // Start over with only the configured hosts under a new epoch:
            // entries stamped with the old epoch drop their availability bits
            // on read, so a host that lands on a retired host's slot cannot
            // inherit its 430s. Cached article bodies are kept.
            tracing::warn!(
                event = "availability_registry_compacted",
                registered = identities.len(),
                configured = configured.len(),
                "Hybrid availability registry is full; starting a new epoch with only the configured hosts. Cached 430 facts are discarded once."
            );
            epoch = RegistryEpoch::fresh(Some(epoch));
            identities = configured;
            changed = true;
        } else if unregistered > 0 {
            for identity in configured {
                if !identities.contains(&identity) {
                    identities.push(identity);
                }
            }
            changed = true;
        }
        if identities.len() > usize::BITS as usize {
            // Config validation caps servers at the bitmap width, so the
            // configured hosts alone always fit.
            anyhow::bail!("hybrid availability registry has no free slot");
        }
        if changed {
            publish_registry(&path, epoch, &identities)?;
        }

        let mut backend_slots = Vec::with_capacity(servers.len());
        for server in servers {
            let identity = AvailabilityIdentity::for_server(server);
            let index = identities
                .iter()
                .position(|candidate| candidate == &identity)
                .expect("configured identity is in registry");
            let slot =
                AvailabilitySlot::new(index).ok_or(AvailabilityLayoutError::TooManyIdentities)?;
            backend_slots.push(slot);
        }
        let slots = Arc::new(BackendSlots::new(&backend_slots));
        Ok(Self {
            miss_sampler: Arc::new(super::miss_sampler::MissSampler::new(Arc::clone(&slots))),
            backend_slots: slots,
            identities: identities.into_boxed_slice(),
            epoch: LayoutEpoch::Registered(epoch),
        })
    }

    #[must_use]
    pub(crate) fn slot_for_backend(&self, backend: BackendId) -> AvailabilitySlot {
        self.backend_slots.get(backend)
    }

    /// Recent misses on shared backbones, for backbone detection's audits.
    #[must_use]
    pub(crate) fn miss_sampler(&self) -> Arc<super::miss_sampler::MissSampler> {
        Arc::clone(&self.miss_sampler)
    }

    /// The table the router reads, so a [`Self::reassign`] reaches it.
    #[must_use]
    pub(crate) fn shared_slots(&self) -> Arc<BackendSlots> {
        Arc::clone(&self.backend_slots)
    }

    /// Move a backend's `430`s to another registered identity.
    ///
    /// Facts already recorded under the old identity stay true for it; the
    /// backend just stops reading and writing them. Returns `false` when the
    /// identity was never registered, which needs a restart to add.
    pub(crate) fn reassign(&self, backend: BackendId, identity: &AvailabilityIdentity) -> bool {
        match self.slot_for_identity(identity) {
            Some(slot) => {
                self.backend_slots.set(backend, slot);
                true
            }
            None => false,
        }
    }

    /// The identity a backend currently records under.
    #[must_use]
    pub(crate) fn identity_for_backend(&self, backend: BackendId) -> &AvailabilityIdentity {
        &self.identities[self.slot_for_backend(backend).index()]
    }

    #[must_use]
    pub(crate) const fn identity_count(&self) -> usize {
        self.identities.len()
    }

    #[must_use]
    pub(crate) fn slot_for_identity(
        &self,
        identity: &AvailabilityIdentity,
    ) -> Option<AvailabilitySlot> {
        self.identities
            .iter()
            .position(|candidate| candidate == identity)
            .and_then(AvailabilitySlot::new)
    }

    #[must_use]
    pub(crate) fn identities(&self) -> &[AvailabilityIdentity] {
        &self.identities
    }

    #[must_use]
    pub(crate) fn fingerprint(&self) -> u64 {
        let mut hasher = XxHash64::default();
        let mut identities = self.identities.to_vec();
        identities.sort_unstable();
        for identity in &identities {
            hasher.write(identity.as_host().as_bytes());
            hasher.write_u8(0);
        }
        hasher.finish().max(1)
    }

    #[must_use]
    pub(crate) fn availability_epoch(&self) -> u64 {
        match self.epoch {
            LayoutEpoch::Derived => self.fingerprint(),
            LayoutEpoch::Registered(epoch) => epoch.0.get(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AvailabilityLayoutError {
    TooManyIdentities,
}

impl fmt::Display for AvailabilityLayoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyIdentities => write!(f, "too many availability identities"),
        }
    }
}

impl std::error::Error for AvailabilityLayoutError {}

fn parse_registry(data: &[u8]) -> Result<(Vec<AvailabilityIdentity>, RegistryEpoch)> {
    if data.len() < REGISTRY_MAGIC.len() + 2 * size_of::<u64>()
        || &data[..REGISTRY_MAGIC.len()] != REGISTRY_MAGIC
    {
        anyhow::bail!("invalid hybrid availability registry");
    }
    let mut cursor = REGISTRY_MAGIC.len();
    let epoch = RegistryEpoch(
        NonZeroU64::new(read_u64(data, &mut cursor)?)
            .context("invalid hybrid availability registry epoch")?,
    );
    let count = usize::try_from(read_u64(data, &mut cursor)?)?;
    if count > usize::BITS as usize {
        anyhow::bail!("hybrid availability registry has too many identities");
    }
    let mut identities = Vec::with_capacity(count);
    for _ in 0..count {
        let host = read_string(data, &mut cursor, "host")?;
        let identity = AvailabilityIdentity::from_persisted_host(host)?;
        if identities.contains(&identity) {
            anyhow::bail!("duplicate hybrid availability identity");
        }
        identities.push(identity);
    }
    if cursor != data.len() {
        anyhow::bail!("trailing hybrid availability registry bytes");
    }
    Ok((identities, epoch))
}

fn publish_registry(
    path: &Path,
    epoch: RegistryEpoch,
    identities: &[AvailabilityIdentity],
) -> Result<()> {
    let mut data = Vec::new();
    data.extend_from_slice(REGISTRY_MAGIC);
    data.extend_from_slice(&epoch.0.get().to_le_bytes());
    data.extend_from_slice(&(identities.len() as u64).to_le_bytes());
    for identity in identities {
        write_string(&mut data, identity.as_host())?;
    }
    let temporary = path.with_extension("registry.tmp");
    fs::write(&temporary, data).with_context(|| format!("write {}", temporary.display()))?;
    if let Err(error) = crate::io_util::atomic_replace_file(&temporary, path) {
        let _ = fs::remove_file(&temporary);
        return Err(error).with_context(|| format!("publish {}", path.display()));
    }
    Ok(())
}

fn write_string(data: &mut Vec<u8>, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    if bytes.len() > MAX_REGISTRY_FIELD_BYTES {
        anyhow::bail!("hybrid availability identity field is too long");
    }
    data.extend_from_slice(&(u32::try_from(bytes.len())?).to_le_bytes());
    data.extend_from_slice(bytes);
    Ok(())
}

fn read_string(data: &[u8], cursor: &mut usize, field: &str) -> Result<String> {
    let length = usize::try_from(read_u32(data, cursor)?)?;
    if length == 0 || length > MAX_REGISTRY_FIELD_BYTES {
        anyhow::bail!("invalid hybrid availability {field} length");
    }
    let end = cursor
        .checked_add(length)
        .context("identity length overflow")?;
    let bytes = data
        .get(*cursor..end)
        .ok_or_else(|| anyhow::anyhow!("truncated hybrid availability {field}"))?;
    *cursor = end;
    Ok(String::from_utf8(bytes.to_vec())?)
}

fn read_u32(data: &[u8], cursor: &mut usize) -> Result<u32> {
    let bytes = data
        .get(*cursor..cursor.saturating_add(size_of::<u32>()))
        .ok_or_else(|| anyhow::anyhow!("truncated hybrid availability u32"))?;
    *cursor += size_of::<u32>();
    Ok(u32::from_le_bytes(bytes.try_into()?))
}

fn read_u64(data: &[u8], cursor: &mut usize) -> Result<u64> {
    let bytes = data
        .get(*cursor..cursor.saturating_add(size_of::<u64>()))
        .ok_or_else(|| anyhow::anyhow!("truncated hybrid availability u64"))?;
    *cursor += size_of::<u64>();
    Ok(u64::from_le_bytes(bytes.try_into()?))
}
