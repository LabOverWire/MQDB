// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

pub(super) fn push_length_prefixed(buf: &mut Vec<u8>, field: &[u8], narrow: bool) -> Option<()> {
    if narrow {
        buf.push(u8::try_from(field.len()).ok()?);
    } else {
        buf.extend_from_slice(&u16::try_from(field.len()).ok()?.to_be_bytes());
    }
    buf.extend_from_slice(field);
    Some(())
}

pub(super) struct WireReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> WireReader<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    pub(super) fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let end = self.offset.checked_add(len)?;
        let slice = self.bytes.get(self.offset..end)?;
        self.offset = end;
        Some(slice)
    }

    pub(super) fn u8(&mut self) -> Option<u8> {
        self.take(1)?.first().copied()
    }

    pub(super) fn u16(&mut self) -> Option<u16> {
        Some(u16::from_be_bytes(self.take(2)?.try_into().ok()?))
    }

    pub(super) fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }

    pub(super) fn u64(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }

    pub(super) fn length(&mut self, wide: bool) -> Option<usize> {
        if wide {
            self.u16().map(usize::from)
        } else {
            self.u8().map(usize::from)
        }
    }

    pub(super) fn text(&mut self, len: usize) -> Option<String> {
        String::from_utf8(self.take(len)?.to_vec()).ok()
    }

    pub(super) fn is_exhausted(&self) -> bool {
        self.offset >= self.bytes.len()
    }
}
