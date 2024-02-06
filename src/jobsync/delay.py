import time


class NonBlockingDelay:
    """Non blocking delay class"""
    def __init__(self):
        self._timestamp = 0
        self._delay = 0

    def _seconds(self):
        return int(time.time())

    def timeout(self):
        """Check if time is up"""
        return (self._seconds() - self._timestamp) > self._delay

    def delay(self, delay):
        """Non blocking delay in seconds"""
        self._timestamp = self._seconds()
        self._delay = delay


def delay(seconds):
    delay = NonBlockingDelay()
    delay.delay(seconds)
    while not delay.timeout():
        continue
