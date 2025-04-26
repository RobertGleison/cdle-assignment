import time
import functools
import datetime
import os

def timer(log_file="logs/general.log"):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()

            elapsed_time = end_time - start_time
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_message = f"\n[{timestamp}] Function '{func.__name__}' took {elapsed_time:.6f} seconds to execute"
            log_dir = os.path.dirname(log_file)

            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir)

            with open(log_file, "a") as f:
                f.write(log_message)

            print(log_message)

            return result
        return wrapper

    # Handle both @time_decorator and @time_decorator() syntax
    if callable(log_file):
        func = log_file
        log_file = "logs/general.log"
        return decorator(func)
    return decorator

# Example usage:
# @timer(log_file="logs/joblib.log")  # Specify custom log file
# def another_example(n):
#    return sum(range(n))
