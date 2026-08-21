use std::sync::LazyLock;

use rayon::prelude::*;

const MAX_QUERY_WORKERS: usize = 4;

static QUERY_WORKERS: LazyLock<Option<rayon::ThreadPool>> = LazyLock::new(|| {
    let workers = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .min(MAX_QUERY_WORKERS);
    rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .thread_name(|index| format!("craqle-query-{index}"))
        .build()
        .ok()
});

pub(crate) fn worker_count() -> usize {
    QUERY_WORKERS
        .as_ref()
        .map_or(1, rayon::ThreadPool::current_num_threads)
}

pub(crate) fn map_ordered<T, U, E>(
    input: Vec<T>,
    operation: impl Fn(T) -> Result<U, E> + Send + Sync,
) -> Result<Vec<U>, E>
where
    T: Send,
    U: Send,
    E: Send,
{
    match QUERY_WORKERS.as_ref() {
        Some(pool) => pool.install(|| input.into_par_iter().map(operation).collect()),
        None => input.into_iter().map(operation).collect(),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn shared_pool_is_bounded_and_folds_in_input_order() {
        assert!((1..=4).contains(&super::worker_count()));
        let output = super::map_ordered(vec![3_u8, 1, 2], |value| {
            Ok::<_, std::convert::Infallible>(value * 2)
        })
        .unwrap();
        assert_eq!(output, vec![6, 2, 4]);
    }
}
