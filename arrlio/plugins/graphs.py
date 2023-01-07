import logging
from copy import deepcopy
from datetime import datetime, timezone
from typing import Dict, List
from uuid import uuid4

from arrlio.core import AsyncResult, __tasks__
from arrlio.exc import ArrlioError, GraphError
from arrlio.models import Event, Graph, Task, TaskData, TaskInstance, TaskResult
from arrlio.plugins import base

logger = logging.getLogger("arrlio.plugins.graphs")


class Config(base.Config):
    pass


class Plugin(base.Plugin):
    @property
    def name(self) -> str:
        return "arrlio.graphs"

    def __init__(self, *args, **kwds):
        super().__init__(*args, **kwds)
        self.graphs: Dict[str, List[Graph, int]] = {}

    async def on_init(self):
        logger.info("%s initialization ...", self)

        if "arrlio.events" not in self.app.plugins:
            raise ArrlioError("'arrlio.graphs' plugin requires 'arrlio.events' plugin'")

        await self.app.consume_events(
            "arrlio.graphs",
            self._on_event,
            event_types=["graph:task:send", "graph:task:done"],
        )

        logger.info("%s initialization done", self)

    async def on_close(self):
        await self.app.stop_consume_events("arrlio.graphs")

    async def on_task_result(self, task_instance: TaskInstance, task_result: TaskResult) -> None:
        graph: Graph = task_instance.data.extra.get("graph")
        if graph is None or task_result.exc is not None:
            return

        root: str = next(iter(graph.roots))

        args = (task_result.res,)

        routes = task_result.routes
        if isinstance(routes, str):
            routes = [routes]

        if root in graph.edges:
            for node_id, node_routes in graph.edges[root]:
                if (routes is None and node_routes is None) or (set(routes or []) & set(node_routes or [])):
                    await self._send_graph(
                        Graph(
                            name=graph.name,
                            nodes=graph.nodes,
                            edges=graph.edges,
                            roots={node_id},
                        ),
                        args=args,
                        meta={
                            "graph_source_node": root,
                            "graph_app_id": task_instance.data.extra["graph_app_id"],
                            "graph_id": task_instance.data.extra["graph_id"],
                            "graph_name": graph.name,
                        },
                        root_only=True,
                    )

    async def on_task_done(self, task_instance: TaskInstance, status: dict) -> None:
        graph: Graph = task_instance.data.extra.get("graph")
        if graph is None:
            return

        task_data: TaskData = task_instance.data

        event: Event = Event(
            type="graph:task:done",
            dt=datetime.now(tz=timezone.utc),
            ttl=task_data.event_ttl,
            data={
                "task_id": task_data.task_id,
                "graph_id": task_data.extra["graph_id"],
                "graph_app_id": task_data.extra["graph_app_id"],
            },
        )
        await self.app.send_event(event)

    async def send_graph(
        self,
        graph: Graph,
        args: tuple = None,
        kwds: dict = None,
        meta: dict = None,
    ) -> Dict[str, AsyncResult]:
        """
        Args:
            graph (Graph): ~arrlio.models.Graph.
            args (tuple, optional): ~arrlio.models.Graph root nodes args.
            kwds (dict, optional): ~arrlio.models.Graph root nodes kwds.
            meta (dict, optional): ~arrlio.models.Graph root nodes meta.

        Returns:
            Dict[str, ~arrlio.core.AsyncResult]: Dictionary with AsyncResult objects.
        """

        if not graph.nodes or not graph.roots:
            raise GraphError("Empty graph or missing roots")

        graph_id = f"{uuid4()}"
        graph_app_id = self.app.config.app_id

        extra = {
            "graph_id": graph_id,
            "graph_app_id": graph_app_id,
            "graph_roots": graph.roots,
        }

        graph: Graph = self._init_graph(graph, extra=extra)

        logger.info("%s: send graph %s(%s)", self.app, graph.name, graph_id)

        self.graphs[graph_id] = [graph, 0]
        try:
            task_instances = await self._send_graph(graph, args=args, kwds=kwds, meta=meta)
            return {k: AsyncResult(self.app, task_instance) for k, task_instance in task_instances.items()}
        except (BaseException, Exception):
            self.graphs.pop(graph_id, None)
            raise

    def _init_graph(self, graph: Graph, extra: dict = None) -> Graph:
        extra = extra or {}

        nodes = deepcopy(graph.nodes)
        edges = graph.edges
        roots = graph.roots

        for _, (_, node_kwds) in nodes.items():
            node_kwds.setdefault("task_id", f"{uuid4()}")
            node_kwds.setdefault("extra", {}).update(extra)

        return Graph(graph.name, nodes=nodes, edges=edges, roots=roots)

    def _get_task_instances(self, graph: Graph, root_only: bool = None) -> Dict[str, TaskInstance]:
        task_instances: Dict[str, TaskInstance] = {}
        task_settings = self.app.task_settings

        for node_id, (task_name, node_kwds) in graph.nodes.items():
            if node_id in graph.roots:
                kwds = {**task_settings, **node_kwds}
            elif root_only:
                continue
            else:
                kwds = node_kwds
            if task_name in __tasks__:
                task_instance = __tasks__[task_name].instantiate(**kwds)
            else:
                task_instance = Task(None, task_name).instantiate(**kwds)
            task_instances[node_id] = task_instance

        for node_id, task_instance in task_instances.items():
            task_instance.data.extra["graph"] = Graph(
                graph.name,
                nodes=graph.nodes,
                edges=graph.edges,
                roots={node_id},
            )

        return task_instances

    async def _send_graph(
        self,
        graph: Graph,
        args: tuple = None,
        kwds: dict = None,
        meta: dict = None,
        root_only: bool = None,
    ) -> Dict[str, TaskInstance]:

        task_instances: Dict[str, TaskInstance] = self._get_task_instances(graph, root_only=root_only)

        for node_id in graph.roots:
            task_instance: TaskInstance = task_instances[node_id]
            task_data: TaskData = task_instance.data
            task_data.args += tuple(args or ())
            task_data.kwds.update(kwds or {})
            task_data.meta.update(meta or {})

            logger.info("%s: send %s", self, task_instance.dict(exclude=["data.args", "data.kwds"]))

            await self.app.backend.send_task(task_instance)

            event: Event = Event(
                type="graph:task:send",
                dt=datetime.now(tz=timezone.utc),
                ttl=task_data.event_ttl,
                data={
                    "task_id": task_data.task_id,
                    "graph_id": task_data.extra["graph_id"],
                    "graph_app_id": task_data.extra["graph_app_id"],
                },
            )
            await self.app.send_event(event)

        return task_instances

    async def _on_event(self, event: Event):
        if (graph_id := event.data["graph_id"]) in self.graphs:
            if event.type == "graph:task:send":
                self.graphs[graph_id][1] += 1
            elif event.type == "graph:task:done":
                self.graphs[graph_id][1] -= 1
                if self.graphs[graph_id][1] == 0:
                    await self._on_graph_done(graph_id)

    async def _on_graph_done(self, graph_id: str):
        graph: Graph = self.graphs.pop(graph_id)[0]

        logger.info("%s: graph %s(%s) done", self, graph.name, graph_id)

        for (task_name, node_kwds) in graph.nodes.values():
            if task_name in __tasks__:
                task_instance = __tasks__[task_name].instantiate(**node_kwds)
            else:
                task_instance = Task(None, task_name).instantiate(**node_kwds)
            if task_instance.data.result_return:
                await self.app.backend.close_task(task_instance)
