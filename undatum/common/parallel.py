"""Parallel processing utilities for CPU-bound operations."""

import logging
import os
from collections.abc import Iterable, Iterator
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from typing import Any, Callable, Optional


def get_cpu_count() -> int:
    """Get number of CPU cores available.

    Returns:
        Number of CPU cores
    """
    try:
        return os.cpu_count() or 1
    except Exception:
        return 1


def is_cpu_bound_operation(operation: str) -> bool:
    """Determine if an operation is CPU-bound.

    Args:
        operation: Operation name (e.g., 'convert', 'stats', 'search')

    Returns:
        True if operation is CPU-bound, False if I/O-bound
    """
    cpu_bound_operations = {
        "convert",
        "stats",
        "frequency",
        "dedup",
        "search",
        "fill",
        "transform",
        "apply",
    }
    return operation.lower() in cpu_bound_operations


def parallel_map(
    func: Callable[[Any], Any],
    items: Iterable[Any],
    num_threads: Optional[int] = None,
    use_processes: bool = False,
    chunk_size: int = 1,
) -> Iterator[Any]:
    """Apply function to items in parallel.

    Args:
        func: Function to apply to each item
        items: Iterable of items to process
        num_threads: Number of threads/processes (default: CPU count)
        use_processes: If True, use processes instead of threads
        chunk_size: Number of items to process per worker task

    Yields:
        Results in order of completion (may not preserve input order)
    """
    if num_threads is None:
        num_threads = get_cpu_count()

    if num_threads <= 1:
        # Single-threaded: just apply function directly
        for item in items:
            yield func(item)
        return

    items_list = list(items)  # Convert to list for parallel processing

    executor_class = ProcessPoolExecutor if use_processes else ThreadPoolExecutor

    with executor_class(max_workers=num_threads) as executor:
        # Submit all tasks
        futures = []
        for i in range(0, len(items_list), chunk_size):
            chunk = items_list[i : i + chunk_size]
            future = executor.submit(func, chunk)
            futures.append(future)

        # Collect results as they complete
        for future in as_completed(futures):
            try:
                result = future.result()
                if isinstance(result, (list, tuple)):
                    for item in result:
                        yield item
                else:
                    yield result
            except Exception as e:
                logging.error(f"Error in parallel processing: {e}")
                raise


def parallel_process_chunks(
    processor: Callable[[list[Any]], list[Any]],
    chunks: Iterator[list[Any]],
    num_threads: Optional[int] = None,
    use_processes: bool = False,
) -> Iterator[list[Any]]:
    """Process chunks in parallel.

    Args:
        processor: Function that processes a chunk and returns processed chunk
        chunks: Iterator of chunks to process
        num_threads: Number of threads/processes (default: CPU count)
        use_processes: If True, use processes instead of threads

    Yields:
        Processed chunks (order may not be preserved)
    """
    chunks_list = list(chunks)  # Convert to list for parallel processing

    if not chunks_list:
        return

    if num_threads is None:
        num_threads = get_cpu_count()

    if num_threads <= 1:
        # Single-threaded: process sequentially
        for chunk in chunks_list:
            yield processor(chunk)
        return

    executor_class = ProcessPoolExecutor if use_processes else ThreadPoolExecutor

    with executor_class(max_workers=num_threads) as executor:
        # Submit all chunks for processing
        future_to_chunk = {executor.submit(processor, chunk): chunk for chunk in chunks_list}

        # Collect results as they complete
        for future in as_completed(future_to_chunk):
            try:
                result = future.result()
                yield result
            except Exception as e:
                chunk = future_to_chunk[future]
                logging.error(f"Error processing chunk: {e}")
                raise
