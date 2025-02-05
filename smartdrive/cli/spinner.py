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

import sys
import time
from threading import Thread


class Spinner:
    def __init__(self, message="Processing"):
        self.message = message
        self.done = False
        self.spinner_thread = Thread(target=self._spin)
        self.current_cursor = '|'

    def _spin(self):
        while not self.done:
            for cursor in '|/-\\':
                sys.stdout.write(f'\r{self.message}... {cursor}')
                sys.stdout.flush()
                self.current_cursor = cursor
                time.sleep(0.1)
        sys.stdout.flush()

    def start(self):
        self.done = False
        self.spinner_thread.start()

    def stop(self):
        self.done = True
        self.spinner_thread.join()
        sys.stdout.write(f'\r{self.message}... done\n')
        sys.stdout.flush()

    def stop_with_message(self, final_message):
        self.done = True
        self.spinner_thread.join()
        sys.stdout.write(f'\r{self.message}... {final_message}\n')
        sys.stdout.flush()
