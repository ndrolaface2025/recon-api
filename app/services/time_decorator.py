import time
import functools
from loguru import logger 
from datetime import datetime

def timing_decorator_async(func):
    """
    Async decorator to measure and log the execution time of async functions.
    Logs start time, end time, and total duration without changing function logic.
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        start_datetime = datetime.now()
        
        # Log function start
        logger.info(f"Async Function '{func.__name__}' started at {start_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
        
        try:
            # Execute the original async function
            result = await func(*args, **kwargs)
            
            end_time = time.time()
            end_datetime = datetime.now()
            duration = end_time - start_time
            
            # Log function completion with timing details
            logger.info(f"Async Function '{func.__name__}' completed at {end_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
            logger.info(f"Async Function '{func.__name__}' took {duration:.4f} seconds ({duration*1000:.2f} ms)")
            
            return result
            
        except Exception as e:
            end_time = time.time()
            end_datetime = datetime.now()
            duration = end_time - start_time
            
            # Log function error with timing details
            logger.error(f"Async Function '{func.__name__}' failed at {end_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} after {duration:.4f} seconds")
            logger.error(f"Error in '{func.__name__}': {str(e)}")
            raise
    
    return wrapper