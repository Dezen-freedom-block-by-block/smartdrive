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

from multiprocessing import Manager
from multiprocessing.managers import ListProxy
from typing import Union, List

from smartdrive.models.event import RemoveEvent, StoreEvent
from smartdrive.validator.node.connection.utils.lock_proxy_wrapper import LockProxyWrapper


class EventPool:

    def __init__(self, manager: Manager):
        self._events: ListProxy = manager.list()
        self._lock: LockProxyWrapper = manager.Lock()

    def get_all(self) -> ListProxy:
        return self._events

    def append(self, event: Union[StoreEvent, RemoveEvent]):
        with self._lock:
            if event.uuid not in list(map(lambda event: event.uuid, self._events)):
                self._events.append(event)

    def append_multiple(self, events: List[Union[StoreEvent, RemoveEvent]]):
        with self._lock:
            existing_uuids = set(map(lambda event: event.uuid, self._events))
            for event in events:
                if event.uuid not in existing_uuids:
                    self._events.append(event)
                    existing_uuids.add(event.uuid)

    def remove_multiple(self, events: List[Union[StoreEvent, RemoveEvent]]):
        uuids_to_remove = {event.uuid for event in events}
        with self._lock:
            updated_events = [event for event in self._events if event.uuid not in uuids_to_remove]
            self._events[:] = updated_events

    def consume_events(self, count: int) -> List[Union[StoreEvent, RemoveEvent]]:
        items = []
        with self._lock:
            for _ in range(min(count, len(self._events))):
                items.append(self._events.pop(0))
        return items
