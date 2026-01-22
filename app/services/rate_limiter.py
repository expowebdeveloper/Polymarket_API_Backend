"""
Rate limiting and retry utilities for API calls.
Handles 429 errors with exponential backoff and rate limiting.
"""

import asyncio
import httpx
from typing import Callable, Any
import time

# Rate limiting configuration
MAX_RETRIES = 5
RETRY_DELAY_BASE = 2  # Base delay in seconds for exponential backoff
RATE_LIMIT_DELAY = 10  # Additional delay for 429 errors (increased)
MIN_DELAY_BETWEEN_REQUESTS = 0.1  # Minimum delay between requests in seconds

# Global rate limiter state
_last_request_time = 0
_request_lock = asyncio.Lock()


async def rate_limited_request(
    func: Callable,
    *args,
    **kwargs
) -> Any:
    """
    Execute a request with rate limiting and retry logic.
    
    Args:
        func: Async function to execute (e.g., async_client.get)
        *args, **kwargs: Arguments to pass to func
    
    Returns:
        Result of func
    """
    global _last_request_time
    
    # Rate limiting: ensure minimum delay between requests
    async with _request_lock:
        current_time = time.time()
        time_since_last = current_time - _last_request_time
        if time_since_last < MIN_DELAY_BETWEEN_REQUESTS:
            await asyncio.sleep(MIN_DELAY_BETWEEN_REQUESTS - time_since_last)
        _last_request_time = time.time()
    
    # Retry logic with exponential backoff
    last_exception = None
    
    for attempt in range(MAX_RETRIES):
        try:
            result = await func(*args, **kwargs)
            
            # Check for 429 status code in response
            if hasattr(result, 'status_code') and result.status_code == 429:
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAY_BASE * (2 ** attempt) + RATE_LIMIT_DELAY
                    print(f"⚠️ Rate limited (429). Retrying in {delay:.1f}s (attempt {attempt + 1}/{MAX_RETRIES})...")
                    await asyncio.sleep(delay)
                    last_exception = httpx.HTTPStatusError(
                        "Rate limited",
                        request=result.request if hasattr(result, 'request') else None,
                        response=result
                    )
                    continue
                else:
                    # Raise the error if we've exhausted retries
                    result.raise_for_status()
            
            # Raise for other HTTP errors
            if hasattr(result, 'raise_for_status'):
                result.raise_for_status()
            
            return result
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                # Rate limited - use longer delay
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAY_BASE * (2 ** attempt) + RATE_LIMIT_DELAY
                    print(f"⚠️ Rate limited (429). Retrying in {delay:.1f}s (attempt {attempt + 1}/{MAX_RETRIES})...")
                    await asyncio.sleep(delay)
                    last_exception = e
                    continue
            # For other HTTP errors, raise immediately
            raise
        except (httpx.TimeoutException, httpx.RequestError) as e:
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAY_BASE * (2 ** attempt)
                print(f"⚠️ Request error. Retrying in {delay:.1f}s (attempt {attempt + 1}/{MAX_RETRIES})...")
                await asyncio.sleep(delay)
                last_exception = e
                continue
            raise
    
    # If we exhausted retries, raise the last exception
    if last_exception:
        raise last_exception
    raise Exception("Max retries exceeded")


async def rate_limited_gather(
    tasks: list,
    max_concurrent: int = 3,
    delay_between_batches: float = 1.0
) -> list:
    """
    Execute tasks with rate limiting and concurrency control.
    
    Args:
        tasks: List of async tasks
        max_concurrent: Maximum number of concurrent tasks
        delay_between_batches: Delay between batches in seconds
    
    Returns:
        List of results
    """
    results = []
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def execute_with_semaphore(task):
        async with semaphore:
            return await task
    
    # Process in batches
    for i in range(0, len(tasks), max_concurrent):
        batch = tasks[i:i + max_concurrent]
        batch_tasks = [execute_with_semaphore(task) for task in batch]
        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        results.extend(batch_results)
        
        # Delay between batches to avoid rate limits
        if i + max_concurrent < len(tasks):
            await asyncio.sleep(delay_between_batches)
    
    return results
