import argparse
import asyncio
import json
import pathlib
from collections import namedtuple
import logging
from datetime import datetime

Node = namedtuple("Node", ["name", "edges"])
Edge = namedtuple("Edge", ["node", "time"])


RFC_3339_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def get_start_nodes(workflow: dict) -> list:
    start_nodes = []
    for key, val in workflow.items():
        start = val.get("start", False)
        if start is True:
            start_nodes.append(key)
    return start_nodes


def is_cyclical(workflow: dict, node: str, visited_nodes: list | None = None) -> bool:
    if visited_nodes is None:
        visited_nodes = []
    if node in visited_nodes:
        return True
    visited_nodes.append(node)
    edges = workflow[node].get("edges", {})
    if edges:
        return any(is_cyclical(workflow, n, visited_nodes) for n in edges)
    return False


def validate_spec(workflow_spec: pathlib.Path) -> None:
    with open(workflow_spec, "r") as fh:
        try:
            workflow = json.loads(fh.read())
        except Exception as err:
            raise TypeError(f"Invalid workflow spec: {err}")
    start_nodes = get_start_nodes(workflow)
    if not start_nodes:
        raise TypeError("Invalid workflow spec: no start nodes found")
    if len(start_nodes) > 1:
        raise TypeError(
            f"Invalid workflow spec: more than one start node found, {start_nodes}"
        )
    if is_cyclical(workflow, start_nodes[0]):
        raise TypeError("Invalid workflow spec: cycle found")


def build_dag(workflow: dict, node_name: str) -> Node:
    edges = workflow[node_name].get("edges", {})
    return Node(
        name=node_name,
        edges=[Edge(node=build_dag(workflow, n), time=edges[n]) for n in edges],
    )


def parse_spec(workflow_spec: pathlib.Path) -> Node:
    with open(workflow_spec, "r") as fh:
        workflow = json.loads(fh.read())
    start_node = get_start_nodes(workflow)[0]
    return build_dag(workflow, start_node)


async def run_workflow_after(delay: float, node: Node, with_timestamps: bool):
    await asyncio.sleep(delay)
    await run_workflow(node, with_timestamps)


async def run_workflow(node: Node, with_timestamps: bool = False) -> None:
    logger = logging.getLogger("traverse.run_workflow")
    timestamp = datetime.utcnow().strftime(RFC_3339_DATETIME_FORMAT)
    logger.info(f"node={node.name}, timestamp={timestamp}")
    print(node.name + (f", {timestamp}" if with_timestamps else ""))
    tasks = [
        asyncio.create_task(run_workflow_after(edge.time, edge.node, with_timestamps))
        for edge in node.edges
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="workflow runner")
    parser.add_argument(
        "workflow_spec",
        type=pathlib.Path,
        help="workflow specification path represented in JSON",
    )
    parser.add_argument(
        "--with-timestamps",
        dest="with_timestamps",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="with_timestamps will print timestamps that the node was visited",
    )
    args = parser.parse_args()
    validate_spec(args.workflow_spec)
    head_node = parse_spec(args.workflow_spec)
    asyncio.run(run_workflow(head_node, args.with_timestamps))
