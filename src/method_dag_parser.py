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
        self.output: Any = None

    def is_executable(self):
        return all(self.upstream.values())

    def execute(self, data: pd.DataFrame, method_map: Dict[str, Callable],
                node_map: Dict[str, Any], **kwargs) -> Any:
        if not self.executed:
            foo = method_map[self.value["variable"]]
            args = []
            kwargs = {}
            for arg in self.args:
                if arg["object"] == "direct":
                    val = arg["variable"]
                elif arg["object"] == "kwargs_obj":
                    val = kwargs[arg["variable"]]
                elif arg["object"] == "data_obj":
                    val = data[arg["variable"]
                               ] if arg["variable"] != "full" else data
                elif arg["object"] == "method":
                    val = node_map[arg["variable"]]
                else:
                    raise ValueError(
                        "Object type %s not recognized." % arg["object"])
                if "name" in arg:
                    kwargs[arg["name"]] = val
                else:
                    args.append(val)
            self.output = foo(*args, **kwargs)
            self.executed = True
        return self.output


class MethodDAG(object):
    def __init__(self, tasks: List[Dict[str, Any]]):
        self.node_map: Dict[str, Any] = {}
        self.nodes: List[Method] = {}
        for task in tasks:
            key = task.pop("key")
            args = task.pop("arguments")
            self.node_map[key] = None
            self.nodes[key] = Method(key, task, args)
        self.queue = []

    def set_dependencies(self, streams: List[Dict[str, Any]]):
        for stream in streams:
            method = self.nodes[stream["task"]]
            for task in stream["downstream"]:
                method.upstream[task] = False
                method.downstream.append(self.nodes[task])

    def set_queue(self):
        for method in self.nodes.values():
            if method.is_executable:
                self.queue.append(method)

    def compute(self, data: pd.DataFrame, method_map: Dict[str, Callable],
                **kwargs) -> Any:
        while len(self.queue) > 0:
            method = self.queue.pop()
            self.node_map[method.key] = method.execute(data, method_map, self.node_map, **kwargs)
            for dependent in method.downstream:
                dependent.upstream[method.key] = True
                if dependent.is_executable():
                    self.queue.append(dependent)

    @classmethod
    def parse(cls, config: Dict[str, Any]) -> MethodDAG:
        dag = cls(config["tasks"])
        dag.set_dependencies(config["streams"])
        dag.set_queue()
        return dag
