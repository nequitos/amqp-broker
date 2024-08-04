from __future__ import annotations

from typing import Any, Callable

from aio_pika import (
    RobustConnection,
    ExchangeType,
    Message
)
from aio_pika.abc import (
    AbstractRobustChannel,
    AbstractRobustExchange,
    AbstractRobustQueue,
    AbstractIncomingMessage,
    ConsumerTag,
    Arguments
)

from asyncio import sleep
from functools import wraps
from contextlib import asynccontextmanager


Callback = Callable[..., Any]


class ExchangeError(Exception):
    def __repr__(self):
        return f"Exchange in broker {__name__} is not declare"


class QueueError(Exception):
    def __repr__(self):
        return f"Queue in broker {__name__} is not declare"


class Broker:

    TIMEOUT: int | float | None = 60.0
    SLEEP_SEC: int | float | None = 3
    DURABLE: bool = True
    AUTO_DELETE: bool = False
    INTERNAL: bool = False
    PASSIVE: bool = False
    EXCLUSIVE: bool = False

    def __init__(
        self,
        name: str | None = None,
        connection: RobustConnection | None = None,
        routing_key: str | None = None
    ):
        if connection is None:
            raise ConnectionError
        if routing_key is None:
            routing_key = name or str(hex(self.__hash__()))

        self._connection = connection
        self.__channel: AbstractRobustChannel | None = None

        self.__exchange: AbstractRobustExchange | None = None
        self.__queue: AbstractRobustQueue | None = None
        self.__routing_key = routing_key

    async def __aenter__(
        self,
        name: str,
        subscribe: str,
        subscribe_type: ExchangeType | str = ExchangeType.DIRECT
    ):
        await self.setup(name, subscribe, subscribe_type)
        self.context()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.__channel.close()

    @property
    def channel(self) -> AbstractRobustChannel | None:
        return self.__channel

    @property
    def exchange(self) -> AbstractRobustExchange | None:
        return self.__exchange

    @property
    def queue(self) -> AbstractRobustQueue | None:
        return self.__queue

    @property
    def routing_key(self) -> str:
        return self.__routing_key

    @asynccontextmanager
    async def context(self) -> Broker:
        try:
            yield self
        finally:
            await self.close()

    async def close(self) -> None:
        if self.channel is not None and not self.channel.is_closed:
            await self.channel.close()

    def subscribe(
        self,
        name: str | None = None,
        subscribe: str | None = None,
        subscribe_type: ExchangeType | str = ExchangeType.DIRECT
    ):
        def wrapper(func):
            @wraps(func)
            async def wrapped(*args, **kwargs):
                await self.setup(name, subscribe, subscribe_type)
                consumer_tag = await self.consume()
                await func(
                    consumer_tag,
                    self.queue,
                    self.exchange,
                    self.routing_key
                )
                return func

            return wrapped

        return wrapper

    async def consume(
        self,
        callback: Callback | None = None,
    ) -> ConsumerTag:
        if callback is None:
            callback = self.on_consume_callback

        consumer_tag = await self.queue.consume(
            callback=callback
        )

        return consumer_tag

    async def publish(
        self,
        message: Message
    ):
        await self.exchange.publish(
            message=message,
            routing_key=self.routing_key
        )

    async def setup(
        self,
        name: str,
        subscribe: str | None = None,
        subscribe_type: ExchangeType | str = ExchangeType.DIRECT,
    ) -> None:
        await self.__open_channel()
        await self.set_queue(name)

        if subscribe is not None:
            await self.set_exchange(
                subscribe=subscribe,
                subscribe_type=subscribe_type
            )

            if self.routing_key is not None:
                await self.queue.bind(
                    exchange=self.exchange,
                    routing_key=self.routing_key
                )

    async def set_exchange(
        self,
        subscribe: str,
        subscribe_type: ExchangeType | str = ExchangeType.DIRECT,
        arguments: Arguments | None = None
    ) -> None:
        self.__exchange = await self.channel.declare_exchange(
            name=subscribe,
            type=subscribe_type,
            durable=self.DURABLE,
            auto_delete=self.AUTO_DELETE,
            internal=self.INTERNAL,
            passive=self.PASSIVE,
            timeout=self.TIMEOUT,
            arguments=arguments
        )

    async def set_queue(
        self,
        name: str | None,
        arguments: Arguments | None = None
    ) -> None:
        self.__queue = await self.channel.declare_queue(
            name=name,
            durable=self.DURABLE,
            exclusive=self.EXCLUSIVE,
            passive=self.PASSIVE,
            auto_delete=self.AUTO_DELETE,
            timeout=self.TIMEOUT,
            arguments=arguments
        )

    async def __open_channel(
        self,
        number: int | None = None,
        confirms: bool = True,
        on_return_raises: bool = False
    ) -> None:
        self.__channel = await self._connection.channel(
            channel_number=number,
            publisher_confirms=confirms,
            on_return_raises=on_return_raises
        )

    async def on_consume_callback(
        self,
        data: AbstractIncomingMessage
    ) -> AbstractIncomingMessage | ConsumerTag | None:
        if await data.ack():
            return data
        if await data.nack():
            return
        if data.processed:
            await sleep(self.SLEEP_SEC)
        if await data.reject():
            return
