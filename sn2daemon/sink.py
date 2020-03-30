import abc
import asyncio
import enum
import typing

import aioxmpp

from hintlib import sample, services, xso


class Sink(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def submit_batch(self, batch: sample.SampleBatch):
        """
        Submit a batch of samples from a single sensor.

        This method must always succeed in the sense that if this sink has an
        internal queue of limited size, it must drop entries as needed to make
        space for this entry.

        That implies that success of this method does not imply that data will
        actually get submitted.
        """

    def submit_batches(self, batches: typing.Iterable[sample.SampleBatch]):
        """
        Submit multiple batches of samples, from possibly different sensors.

        This method must always succeed in the sense that if this sink has an
        internal queue of limited size, it must drop entries as needed to make
        space for this entry.

        That implies that success of this method does not imply that data will
        actually get submitted.
        """
        for batch in batches:
            self.submit_batch(batch)


class MetricCollectorSink(Sink):
    def __init__(self, service: services.BatchSubmitterService):
        super().__init__()
        self._service = service

    def submit_batch(self, batch: sample.SampleBatch):
        return self.submit_batches([batch])

    def submit_batches(self, batches: typing.Iterable[sample.SampleBatch]):
        self._service.enqueue_batches(list(batches))


class PubSubSink(Sink):
    def __init__(self,
                 client: aioxmpp.PubSubClient,
                 service: aioxmpp.JID,
                 queue_size: int = 128):
        super().__init__()
        self._client = client
        self._service = service
        self._worker_task = services.RestartingTask(self._worker)
        self._worker_task.start()
        self._queue = asyncio.Queue(queue_size)
        self._configured_nodes = set()
        self.node_prefix = "https://xmlns.zombofant.net/hint/sensor/1.0#"

    async def _configure_node(self, node: str):
        if node in self._configured_nodes:
            return

        # TODO: do actual configuration here
        try:
            await self._client.create(
                self._service,
                node,
            )
        except aioxmpp.errors.XMPPError as exc:
            if exc.condition != aioxmpp.ErrorCondition.CONFLICT:
                raise

        self._configured_nodes.add(node)

    async def _process_item(self, batch: sample.SampleBatch):
        node = "{}{}".format(self.node_prefix, batch.bare_path)
        payload = xso.SampleBatch()
        payload.timestamp = batch.timestamp
        payload.bare_path = str(batch.bare_path)

        for subpart, value in batch.samples.items():
            sample_xso = xso.NumericSample()
            sample_xso.subpart = subpart
            sample_xso.value = value
            payload.samples.append(sample_xso)

        await self._configure_node(node)

        await self._client.publish(
            self._service,
            node,
            payload,
        )

    async def _worker(self):
        self._configured_nodes.clear()
        while True:
            await self._process_item(await self._queue.get())
            # one item sucessfully published, we can reset any ongoing
            # exponential back off
            self._worker_task.backoff.reset()

    def _enqueue_dropping_old(self, item):
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            to_drop = self._queue.get_nowait()
            self.logger.warning("queue full, dropping %r", to_drop)
            self._drop_item(to_drop)
            self._queue.put_nowait(item)

    def submit_batch(self, batch: sample.SampleBatch):
        self._enqueue_dropping_old(batch)

    def submit_batches(self, batches: typing.Iterable[sample.SampleBatch]):
        most_recent_by_sensor = {}
        for batch in batches:
            path = batch.bare_path
            try:
                curr = most_recent_by_sensor[path]
            except KeyError:
                most_recent_by_sensor[path] = batch
                continue

            if curr.timestamp < batch.timestamp:
                most_recent_by_sensor[path] = batch

        for item in most_recent_by_sensor.values():
            self.submit_batch(item)
