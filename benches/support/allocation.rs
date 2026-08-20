use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

struct CountingAllocator;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static INTERVAL_PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let resized = unsafe { System.realloc(pointer, layout, new_size) };
        if !resized.is_null() {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
            if new_size >= layout.size() {
                record_live_growth(new_size - layout.size());
            } else {
                LIVE_BYTES.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        resized
    }
}

fn record_allocation(size: usize) {
    ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    ALLOCATED_BYTES.fetch_add(size as u64, Ordering::Relaxed);
    record_live_growth(size);
}

fn record_live_growth(size: usize) {
    let live = LIVE_BYTES.fetch_add(size, Ordering::Relaxed) + size;
    INTERVAL_PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
}

#[derive(Clone, Copy, Debug)]
pub struct AllocationSample {
    pub allocations: u64,
    pub allocated_bytes: u64,
    pub peak_live_delta_bytes: usize,
}

pub struct AllocationInterval {
    allocations: u64,
    allocated_bytes: u64,
    live_bytes: usize,
}

impl AllocationInterval {
    pub fn begin() -> Self {
        INTERVAL_PEAK_BYTES.store(0, Ordering::SeqCst);
        let live_bytes = LIVE_BYTES.load(Ordering::SeqCst);
        INTERVAL_PEAK_BYTES.fetch_max(live_bytes, Ordering::SeqCst);
        Self {
            allocations: ALLOCATIONS.load(Ordering::SeqCst),
            allocated_bytes: ALLOCATED_BYTES.load(Ordering::SeqCst),
            live_bytes,
        }
    }

    pub fn finish(self) -> AllocationSample {
        AllocationSample {
            allocations: ALLOCATIONS
                .load(Ordering::SeqCst)
                .saturating_sub(self.allocations),
            allocated_bytes: ALLOCATED_BYTES
                .load(Ordering::SeqCst)
                .saturating_sub(self.allocated_bytes),
            peak_live_delta_bytes: INTERVAL_PEAK_BYTES
                .load(Ordering::SeqCst)
                .saturating_sub(self.live_bytes),
        }
    }
}
