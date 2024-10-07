#  MIT License
#
#  Copyright (c) 2024 Dezen | freedom block by block
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.

import multiprocessing


class LockProxyWrapper:
    """
    A wrapper class around the AcquirerProxy returned by `multiprocessing.Manager().Lock()`.

    Purpose:
    ----------
    The AcquirerProxy class, which is returned by `multiprocessing.Manager().Lock()`, is not
    directly visible or accessible in our project. This class (`LockProxyWrapper`) is created
    as a workaround to wrap the functionality of the AcquirerProxy, allowing us to interact with
    it in a more controlled manner.

    Python abstracts the underlying implementation details of shared objects (like locks)
    in multiprocessing to simplify the developer's experience. While this is generally useful,
    in our case it can lead to the developer not being fully aware of the true nature of
    the variable, which is actually an instance of AcquirerProxy. This wrapper ensures
    transparency by making it clear that the object being manipulated is a proxy, while
    also maintaining full control over the interactions with the lock.
    """
    def __init__(self, manager=None):
        if manager:
            self._lock = manager.Lock()
        else:
            self._lock = multiprocessing.Lock()

    def acquire(self, *args, **kwargs):
        return self._lock.acquire(*args, **kwargs)

    def release(self):
        return self._lock.release()

    def __enter__(self):
        self._lock.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._lock.release()
