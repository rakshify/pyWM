import asyncio
import os
import time

from multiprocessing import Process, Queue, cpu_count, Manager
from typing import Any, Callable, Dict, List, Tuple, Union

import pandas as pd


class InvalidMethodTree(Exception):
    pass


class Method(object):
    def __init__(self, key: str, value: Any, arguments: List[Dict[str, Any]]):
        self.key = key
        self.value = value
        self.args = arguments
        self.upstream: Dict[str, bool] = {}
        self.downstream: List[Method] = []
        self.executed: bool = False
        self.executing: bool = False
        self.output: Any = None

    def is_executable(self):
        return all(self.upstream.values())

    def unique_downstream(self):
        self.downstream = list(set(self.downstream))

    def execute(self, method_map: Dict[str, Callable],
                node_map: Dict[str, Any], **kwargs) -> Any:
        if not self.executed:
            foo = method_map[self.value["variable"]]
            args = []
            kw = {}
            for arg in self.args:
                if arg["object"] == "direct":
                    val = arg["variable"]
                elif arg["object"] == "kwargs_obj":
                    val = kwargs[arg["variable"]]
                elif arg["object"] == "method":
                    val = node_map[arg["variable"]]
                else:
                    raise ValueError(
                        "Object type %s not recognized." % arg["object"])
                if "name" in arg:
                    kw[arg["name"]] = val
                else:
                    args.append(val)
            self.output = foo(*args, **kw)
            self.executing = False
            self.executed = True
        return self.output


class MethodDAG(object):
    def __init__(self, tasks: List[Dict[str, Any]]):
        self.node_map: Dict[str, Any] = {}
        self.nodes: Dict[str, Method] = {}
        self.tasks: Dict[str, bool] = {}
        for task in tasks:
            key = task.pop("key")
            args = task.pop("arguments")
            self.tasks[key] = False
            self.node_map[key] = None
            self.nodes[key] = Method(key, task, args)
        self.queue = []

    def set_dependencies(self, streams: List[Dict[str, Any]]):
        for stream in streams:
            method = self.nodes[stream["task"]]
            for task in stream["downstreams"]:
                method.upstream[task] = False
                self.nodes[task].downstream.append(method)

    def set_queue(self):
        for method in self.nodes.values():
            method.unique_downstream()
            if method.is_executable():
                method.executing = True
                self.queue.append(method)

    def compute(self, method_map: Dict[str, Callable], **kwargs) -> Any:
        self.set_queue()
        while len(self.queue) > 0:
            method = self.queue.pop()
            self.node_map[method.key] = method.execute(
                method_map, self.node_map, **kwargs)
            for dependent in method.downstream:
                dependent.upstream[method.key] = True
                if dependent.is_executable():
                    self.queue.append(dependent)

    @classmethod
    def parse(cls, config: Dict[str, Any]) -> "MethodDAG":
        dag = cls(config["tasks"])
        dag.set_dependencies(config["streams"])
        return dag


class MethodDAGConcurrent(MethodDAG):
    def __init__(self, tasks: List[Dict[str, Any]], n: int = -1):
        super(MethodDAGConcurrent, self).__init__(tasks)
        self.queue = asyncio.Queue()
        if n == -1:
            self.num_processes = cpu_count()
        else:
            self.num_processes = n

    async def produce(self, queue: asyncio.Queue):
        for method in self.nodes.values():
            method.unique_downstream()
            if method.is_executable():
                method.executing = True
                await self.queue.put(method)
        while True:
            complete = all([
                method.executed or method.executing
                for method in self.nodes.values()
            ])
            if complete:
                break
            for method in self.nodes.values():
                execution = method.executed or method.executing
                if self.tasks[method.key]:
                    for dependent in method.downstream:
                        dependent.upstream[method.key] = True
                if method.is_executable() and not execution:
                    method.executing = True
                    await self.queue.put(method)
            await asyncio.sleep(0.001)

    async def consume(self, queue: asyncio.Queue(),
                      method_map: Dict[str, Callable], **kwargs):
        while True:
            method = await self.queue.get()
            self.node_map[method.key] = method.execute(
                method_map, self.node_map, **kwargs)
            self.tasks[method.key] = True
            queue.task_done()

    async def _compute(self, method_map: Dict[str, Callable], **kwargs) -> Any:
        producer = self.produce(self.queue)
        consumers = []
        for i in range(self.num_processes):
            p = asyncio.ensure_future(self.consume(
                self.queue, method_map, **kwargs))
            consumers.append(p)
        await asyncio.gather(producer, return_exceptions=True)
        await self.queue.join()
        for consumer in consumers:
            consumer.cancel()

    def compute(self, method_map: Dict[str, Callable], **kwargs) -> Any:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._compute(method_map, **kwargs))
        loop.close()
