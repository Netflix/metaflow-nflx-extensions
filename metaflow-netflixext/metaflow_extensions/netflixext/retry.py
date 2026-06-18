import time
from functools import wraps


def retry_exp_backoff(
    exceptions_to_check,
    user_msg,
    user_facing_exception=None,
    logger=print,
    tries=6,
    delay=1,
    exponential_backoff_factor=2,
    max_delay_per_retry=10,
):
    """
    Exponential backoff based retry, pass in a tuple of Exceptions on which to retry. All time based
    arguments like ('delay' and 'max_delay_per_retry') are in seconds. By default the logger logs
    to stdout (print).

    Always try to pass 'user_facing_exception' in order to give a nice user facing exceptions in
    the end if retrying does not help.

    Args:
        exceptions_to_check (tuple[Exception]): (required) exceptions on which to retry
        user_msg (str): (required) use facing error message that shows during every retry attempt
        user_facing_exception (Exception): (optional) user facing exception in case all retries fail
        logger (Callable): (optional) callable to print the above 'user_msg' and the final exception
        tries (int): (optional) number of retries
        delay (int): (optional) base delay between retries in seconds
        exponential_backoff_factor (int): (optional) exponential backoff factory between retries
        max_delay_per_retry (int): (optional) max delay between single retries in seconds
    """
    if not isinstance(exceptions_to_check, tuple):
        raise ValueError("'exceptions_to_check' should be a 'tuple' of 'Exception'.")

    if not user_msg:
        raise ValueError("'user_msg' must be specified")

    if user_facing_exception and not isinstance(user_facing_exception, Exception):
        raise ValueError(
            "'user_facing_exception' should be a 'Exception' not '%s'"
            % user_facing_exception.__class__.__name__
        )

    def deco_retry(f):
        @wraps(f)
        def retry_func(*args, **kwargs):
            remaining_tries, retry_delay = tries, min(delay, max_delay_per_retry)

            # Attempt to retry with exponential backoff
            while remaining_tries >= 1:
                try:
                    return f(*args, **kwargs)
                except exceptions_to_check as function_exception:

                    if logger:
                        msg = "%s. Retrying in %d seconds..." % (
                            user_msg,
                            retry_delay,
                        )
                        logger(msg)

                    # All retries failed
                    if remaining_tries == 1:
                        logger(str(function_exception))
                        raise user_facing_exception or function_exception

                    time.sleep(retry_delay)

                    # Update params for the next retry
                    remaining_tries -= 1
                    retry_delay *= exponential_backoff_factor
                    retry_delay = min(retry_delay, max_delay_per_retry)

        return retry_func

    return deco_retry
