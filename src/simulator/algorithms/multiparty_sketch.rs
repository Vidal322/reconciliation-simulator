use std::mem;

use std::hash::{DefaultHasher, Hash, Hasher};

use crate::simulator::algorithms::riblt::SymbolMapping;
use crate::simulator::replica::Replica;

// ---------------------------------------------------------------------------
// Finite prime field GF(P)
//
// P is the largest 64-bit prime.  Using a prime field instead of XOR means
// that k copies of the same element contribute k·e to the symbol accumulator
// rather than eXOReXOR…, which cancels to 0 for even k.  With field arithmetic
// we can always recover e = sym / k via modular inversion.
// ---------------------------------------------------------------------------
pub const P: u64 = 18_446_744_073_709_551_557;

#[inline]
pub fn fadd(a: u64, b: u64) -> u64 {
    let (s, ov) = a.overflowing_add(b);
    if ov || s >= P { s.wrapping_sub(P) } else { s }
}

#[inline]
pub fn fsub(a: u64, b: u64) -> u64 {
    if a >= b { a - b } else { P - (b - a) }
}

#[inline]
pub fn fmul(a: u64, b: u64) -> u64 {
    ((a as u128 * b as u128) % P as u128) as u64
}

pub fn fpow(mut base: u64, mut exp: u64) -> u64 {
    let mut result = 1u64;
    while exp > 0 {
        if exp & 1 == 1 {
            result = fmul(result, base);
        }
        exp >>= 1;
        base = fmul(base, base);
    }
    result
}

/// Modular inverse via Fermat's little theorem (P is prime).
#[inline]
pub fn finv(a: u64) -> u64 {
    fpow(a, P - 2)
}

#[inline]
pub fn fdiv(a: u64, b: u64) -> u64 {
    fmul(a, finv(b))
}

/// Convert a signed count to its representative in GF(P).
#[inline]
pub fn count_to_field(count: i64) -> u64 {
    if count >= 0 {
        (count as u64) % P
    } else {
        P - ((-count) as u64 % P)
    }
}

/// Deterministic hash of a digest, used as the mapping seed and for cell
/// verification.  Must be the same function used during encoding and peeling.
#[inline]
pub fn digest_hash(d: u64) -> u64 {
    let mut h = DefaultHasher::new();
    d.hash(&mut h);
    h.finish()
}

// Serialised size of one coded symbol: sym (8 B) + hash (8 B) + count (8 B).
pub const CELL_BYTES: usize = 3 * mem::size_of::<u64>();

// ---------------------------------------------------------------------------
// Multiparty coded symbol
// ---------------------------------------------------------------------------

/// A single coded symbol cell that accumulates element contributions using
/// field arithmetic rather than XOR.
#[derive(Clone, Default)]
pub struct MCell {
    /// Field sum of (digest % P) for each element inserted (+) or removed (−).
    pub sym: u64,
    /// Field sum of (digest_hash(digest) % P) for each element.
    pub hash: u64,
    /// Signed count: +1 per insertion, −1 per removal.
    pub count: i64,
}

impl MCell {
    /// A cell is pure when it encodes exactly |count| copies of a single
    /// element.  We verify by recovering the candidate element via field
    /// division and checking consistency with the hash accumulator.
    pub fn is_pure(&self) -> bool {
        if self.count == 0 {
            return false;
        }
        let kf = count_to_field(self.count);
        let e_cand = fdiv(self.sym, kf);
        let h_cand = digest_hash(e_cand) % P;
        fmul(h_cand, kf) == self.hash
    }

    /// Recover the field-reduced digest from a pure cell.
    pub fn recover_digest_f(&self) -> u64 {
        fdiv(self.sym, count_to_field(self.count))
    }
}

// ---------------------------------------------------------------------------
// Multiparty sketch
// ---------------------------------------------------------------------------

pub struct MSketch {
    pub cells: Vec<MCell>,
}

impl MSketch {
    pub fn new(size: usize) -> Self {
        Self {
            cells: vec![MCell::default(); size],
        }
    }

    /// Insert (+1) or remove (−1) one element from the sketch.
    pub fn encode_element(&mut self, digest: u64, sign: i64) {
        let e_f = digest % P;
        let h_f = digest_hash(digest) % P;
        let size = self.cells.len();
        let mut mapping = SymbolMapping::new(digest_hash(digest));
        loop {
            let idx = mapping.next().unwrap();
            if idx >= size {
                break;
            }
            if sign > 0 {
                self.cells[idx].sym = fadd(self.cells[idx].sym, e_f);
                self.cells[idx].hash = fadd(self.cells[idx].hash, h_f);
                self.cells[idx].count += 1;
            } else {
                self.cells[idx].sym = fsub(self.cells[idx].sym, e_f);
                self.cells[idx].hash = fsub(self.cells[idx].hash, h_f);
                self.cells[idx].count -= 1;
            }
        }
    }

    /// In-place field addition: self += other (aggregation step).
    pub fn add_sketch(&mut self, other: &MSketch) {
        for (a, b) in self.cells.iter_mut().zip(&other.cells) {
            a.sym = fadd(a.sym, b.sym);
            a.hash = fadd(a.hash, b.hash);
            a.count += b.count;
        }
    }

    /// In-place field subtraction: self −= other (diff step).
    pub fn sub_sketch(&mut self, other: &MSketch) {
        for (a, b) in self.cells.iter_mut().zip(&other.cells) {
            a.sym = fsub(a.sym, b.sym);
            a.hash = fsub(a.hash, b.hash);
            a.count -= b.count;
        }
    }

    /// Greedy peeling decoder.
    ///
    /// Returns the field-reduced digests of "remote-only" elements
    /// (those with count > 0 after diff) if the sketch fully decodes,
    /// or `None` if the sketch is too small.
    pub fn try_decode(&self) -> Option<Vec<u64>> {
        let mut cells = self.cells.clone();
        let size = cells.len();
        let mut remote_only = Vec::new();
        let mut changed = true;

        while changed {
            changed = false;
            for i in 0..size {
                if cells[i].count == 0 || !cells[i].is_pure() {
                    continue;
                }

                let e_f = cells[i].recover_digest_f();
                if cells[i].count > 0 {
                    remote_only.push(e_f);
                }

                // Peel: subtract this element's total contribution from every
                // cell it maps to.  For a pure cell the deltas equal the cell
                // values themselves.
                let sym_d = cells[i].sym;
                let hash_d = cells[i].hash;
                let cnt_d = cells[i].count;

                // Use the field-reduced digest as the mapping seed; for
                // digests < P (essentially all u64 values) this equals the
                // original digest so the mapping matches encoding exactly.
                let mut mapping = SymbolMapping::new(digest_hash(e_f));
                loop {
                    let idx = mapping.next().unwrap();
                    if idx >= size {
                        break;
                    }
                    cells[idx].sym = fsub(cells[idx].sym, sym_d);
                    cells[idx].hash = fsub(cells[idx].hash, hash_d);
                    cells[idx].count -= cnt_d;
                }

                changed = true;
            }
        }

        if cells.iter().all(|c| c.count == 0) {
            Some(remote_only)
        } else {
            None
        }
    }
}

pub fn build_sketch(replica: &Replica, size: usize) -> MSketch {
    let mut sketch = MSketch::new(size);
    for elem in &replica.set {
        sketch.encode_element(elem.digest, 1);
    }
    sketch
}
