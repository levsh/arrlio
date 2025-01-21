import logging

from copy import deepcopy
from datetime import datetime, timezone
from typing import Callable, cast
from uuid import uuid4

from arrlio import AsyncResult, registered_tasks, settings
from arrlio.exceptions import ArrlioError, GraphError
from arrlio.models import Event, Graph, Task, TaskInstance, TaskResult
from arrlio.plugins import base
from arrlio.types import Args, Kwds
from arrlio.utils import is_info_level


logger = logging.getLogger("arrlio.plugins.graphs")


class Config(base.Config):
    """
    Graphs Plugin config.
    """


class Plugin(base.Plugin):
    """
    Graphs Plugin.

    Args:
        app: `App` instance.
        config: Graphs Plugin config.
    """

    def __init__(self, app, config: Config):
        super().__init__(app, config)
        self.graphs: dict[str, tuple[Graph, dict[str, int]]] = {}

    @property
    def name(self) -> str:
        """Plugin name."""

        return "arrlio.graphs"

    @property
    def event_types(self) -> list[str]:
        return [
            "graph.task.send",
            "graph.task.done",
        ]

    async def on_init(self):
        logger.info("%s initializing...", self)

        if "arrlio.events" not in self.app.plugins:
            raise ArrlioError("'arrlio.graphs' plugin depends on 'arrlio.events' plugin'")

        await self.app.consume_events(
            "arrlio.graphs",
            self._on_event,
            event_types=["graph.task.send", "graph.task.done"],
        )

        logger.info("%s initialization done", self)

    async def on_close(self):
        await self.app.stop_consume_events("arrlio.graphs")

    async def on_task_result(self, task_instance: TaskInstance, task_result: TaskResult) -> None:
        headers = task_instance.headers
        graph: Graph = cast(Graph, headers.get("graph:graph"))

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
                            "graph:source_node": root,
                            "graph:app_id": headers["graph:app_id"],
                            "graph:id": headers["graph:id"],
                            "graph:name": graph.name,
                        },
                        root_only=True,
                    )

    async def on_task_done(self, task_instance: TaskInstance, task_result: TaskResult) -> None:
        headers = task_instance.headers
        graph: Graph = cast(Graph, headers.get("graph:graph"))
        if graph is None:
            return

        event: Event = Event(
            type="graph.task.done",
            dt=datetime.now(tz=timezone.utc),
            ttl=task_instance.event_ttl,
            data={
                "task:id": task_instance.task_id,
                "graph:id": headers["graph:id"],
                "graph:app_id": headers["graph:app_id"],
                "graph:call_id": headers["graph:call_id"],
            },
        )
        await self.app.send_event(event)

    async def send_graph(
        self,
        graph: Graph,
        args: Args | None = None,
        kwds: Kwds | None = None,
        meta: dict | None = None,
    ) -> dict[str, AsyncResult]:
        """
        Args:
            graph: Graph to send.
            args: Root nodes positional arguments.
            kwds: Root nodes keyword arguments.
            meta: Root nodes `meta` keyword argument.

        Returns:
            Mapping or AsyncResult objects.
        """

        if not graph.nodes or not graph.roots:
            raise GraphError("empty graph or missing roots")

        graph_id = f"{uuid4()}"
        graph_app_id = self.app.config.app_id

        headers = {
            "arrlio:closable": True,
            "graph:id": graph_id,
            "graph:app_id": graph_app_id,
            "graph:roots": graph.roots,
        }

        graph = self._init_graph(graph, headers=headers)

        logger.info("%s send graph %s[%s]", self, graph.name, graph_id)

        self.graphs[graph_id] = (graph, {})
        try:
            task_instances: dict[str, TaskInstance] = await self._send_graph(graph, args=args, kwds=kwds, meta=meta)
            return {k: AsyncResult(self.app, task_instance) for k, task_instance in task_instances.items()}
        except (BaseException, Exception):
            del self.graphs[graph_id]
            raise

    def _init_graph(self, graph: Graph, headers: dict | None = None) -> Graph:
        headers = headers or {}

        nodes = deepcopy(graph.nodes)
        edges = graph.edges
        roots = graph.roots

        for _, (_, node_kwds) in nodes.items():
            node_kwds.setdefault("task_id", f"{uuid4()}")
            node_kwds.setdefault("headers", {}).update(headers)

        return Graph(graph.name, nodes=nodes, edges=edges, roots=roots)

    def _build_task_instances(self, graph: Graph, root_only: bool | None = None) -> dict[str, TaskInstance]:
        task_instances: dict[str, TaskInstance] = {}
        task_settings = self.app.task_settings

        for node_id, (task_name, node_kwds) in graph.nodes.items():
            if node_id in graph.roots:
                kwds = {**task_settings, **node_kwds}
            elif root_only:
                continue
            else:
                kwds = node_kwds
            if task_name in registered_tasks:
                task_instance = registered_tasks[task_name].instantiate(**kwds)
            else:
                task_instance = Task(cast(Callable, None), task_name).instantiate(**kwds)

            task_instances[node_id] = task_instance

        for node_id, task_instance in task_instances.items():
            task_instance.headers["graph:graph"] = Graph(
                graph.name,
                nodes=graph.nodes,
                edges=graph.edges,
                roots={node_id},
            )

        return task_instances

    async def _send_graph(
        self,
        graph: Graph,
        args: Args | None = None,
        kwds: Kwds | None = None,
        meta: dict | None = None,
        root_only: bool | None = None,
    ) -> dict[str, TaskInstance]:
        task_instances: dict[str, TaskInstance] = self._build_task_instances(graph, root_only=root_only)

        for node_id in graph.roots:
            task_instance: TaskInstance = task_instances[node_id]
            object.__setattr__(task_instance, "args", tuple(task_instance.args) + tuple(args or ()))
            task_instance.kwds.update(kwds or {})
            task_instance.meta.update(meta or {})
            headers = task_instance.headers
            headers["graph:call_id"] = f"{uuid4()}"

            if is_info_level():
                logger.info(
                    "%s send graph '%s' task\n%s",
                    self,
                    graph.name,
                    task_instance.pretty_repr(sanitize=settings.LOG_SANITIZE),
                )

            task_instance.headers.update(self.app.result_backend.make_headers(task_instance))
            task_instance.shared.update(self.app.result_backend.make_shared(task_instance))

            await self.app.broker.send_task(task_instance)

            await self.app.result_backend.allocate_storage(task_instance)

            event: Event = Event(
                type="graph.task.send",
                dt=datetime.now(tz=timezone.utc),
                ttl=task_instance.event_ttl,
                data={
                    "task:id": task_instance.task_id,
                    "graph:id": headers["graph:id"],
                    "graph:app_id": headers["graph:app_id"],
                    "graph:call_id": headers["graph:call_id"],
                },
            )
            await self.app.send_event(event)

        return task_instances

    async def _on_event(self, event: Event):
        if (graph_id := event.data["graph:id"]) in self.graphs:
            task_id = event.data["task:id"]
            item = self.graphs[graph_id]
            item[1].setdefault(task_id, 0)
            if event.type == "graph.task.send":
                item[1][task_id] += 1
            elif event.type == "graph.task.done":
                item[1][task_id] -= 1
                if item[1][task_id] == 0:
                    del item[1][task_id]
                if not item[1]:
                    await self._on_graph_done(graph_id)

    async def _on_graph_done(self, graph_id: str):
        graph: Graph = self.graphs.pop(graph_id)[0]

        logger.info("%s graph %s[%s] done", self, graph.name, graph_id)

        for task_name, node_kwds in graph.nodes.values():
            if task_name in registered_tasks:
                task_instance = registered_tasks[task_name].instantiate(**node_kwds)
            else:
                task_instance = Task(cast(Callable, None), task_name).instantiate(**node_kwds)
            if task_instance.result_return:
                await self.app.result_backend.close_task(task_instance)
