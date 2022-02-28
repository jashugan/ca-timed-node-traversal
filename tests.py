import asyncio
import json
import os
import pathlib
import re
import subprocess
import unittest
from datetime import datetime, timedelta
from re import Pattern

from main import (
    validate_spec,
    parse_spec,
    Node,
    Edge,
    run_workflow,
    RFC_3339_DATETIME_FORMAT,
)

FIXTURES = {
    "SIMPLE": json.dumps(
        {
            "A": {"start": True, "edges": {"B": 0.5, "C": 0.7}},
            "B": {"edges": {}},
            "C": {"edges": {}},
        }
    ),
    "TRIVIAL": json.dumps(
        {
            "A": {"start": True, "edges": {}},
        }
    ),
    "INTERLEAVED": json.dumps(
        {
            "A": {"start": True, "edges": {"B": 0.4, "C": 0.7}},
            "B": {"edges": {"D": 0.1}},
            "C": {"edges": {}},
            "D": {"edges": {}},
        }
    ),
    "TWO_STARTS": json.dumps(
        {"A": {"start": True, "edges": {"B": 0.1}}, "B": {"start": True}}
    ),
    "NO_STARTS": json.dumps({"A": {"edges": {"B": 0.1}}, "B": {}}),
    "CYCLICAL": json.dumps(
        {
            "A": {"start": True, "edges": {"B": 0.1}},
            "B": {"edges": {"A": 0.1}},
        }
    ),
    "INVALID": json.dumps({"A": {"start": True, "edges": {}}})[:-3],
}


def setup_fixture(fixture_key: str, file_path: pathlib.Path) -> None:
    with open(file_path, "w") as fh:
        fh.write(FIXTURES[fixture_key])


def parse_timestamp(rfc3339_timestamp: str) -> datetime:
    return datetime.strptime(rfc3339_timestamp, RFC_3339_DATETIME_FORMAT)


def parse_output(output: [str], pattern: Pattern) -> ([str], [float]):
    nodes_visited = []
    timestamps = []
    for line in output:
        mo = pattern.match(line)
        nodes_visited.append(mo.group(1))
        timestamps.append(parse_timestamp(mo.group(2)))
    timings = []
    start_timestamp = timestamps[0]
    for timestamp in timestamps[1:]:
        td = timestamp - start_timestamp
        delta_in_seconds = round(td / timedelta(seconds=1), 1)
        timings.append(delta_in_seconds)
    return nodes_visited, timings


class RunnerTest(unittest.TestCase):
    LOG_PATTERN = re.compile(r"(\w+), (.*)")

    def setUp(self) -> None:
        self.file_path = pathlib.Path("tests-runner-fixture.json")

    def tearDown(self) -> None:
        os.remove(self.file_path)

    def run_script(self) -> (list, str):
        process = subprocess.Popen(
            f"python main.py --with-timestamps {self.file_path}",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
        )
        outs, errs = process.communicate()
        outs = outs.decode("utf-8").strip().split("\n")
        errs = errs.decode("utf-8").strip()
        return outs, errs

    def test_simple_json_file_runs_correctly(self) -> None:
        setup_fixture("SIMPLE", self.file_path)
        outs, errs = self.run_script()
        self.assertFalse(errs)
        nodes_visited, timings = parse_output(outs, self.LOG_PATTERN)
        self.assertListEqual(nodes_visited, ["A", "B", "C"])
        self.assertListEqual(timings, [0.5, 0.7])

    def test_invalid_fixture_raises_error(self) -> None:
        setup_fixture("INVALID", self.file_path)
        outs, errs = self.run_script()
        self.assertTrue(errs)
        self.assertIn("invalid", errs.lower())


class ValidateSpecTest(unittest.TestCase):
    def setUp(self) -> None:
        self.file_path = pathlib.Path("tests-validate-fixture.json")

    def tearDown(self) -> None:
        os.remove(self.file_path)

    def test_valid_fixture(self) -> None:
        setup_fixture("TRIVIAL", self.file_path)
        self.assertIsNone(validate_spec(self.file_path))

    def test_two_starts_fixture_raises_error(self) -> None:
        setup_fixture("TWO_STARTS", self.file_path)
        self.assertRaises(TypeError, validate_spec, self.file_path)

    def test_no_starts_fixture_raises_error(self) -> None:
        setup_fixture("NO_STARTS", self.file_path)
        self.assertRaises(TypeError, validate_spec, self.file_path)

    def test_cyclical_fixture_raises_error(self) -> None:
        setup_fixture("CYCLICAL", self.file_path)
        self.assertRaises(TypeError, validate_spec, self.file_path)

    def test_invalid_fixture_raises_error(self) -> None:
        setup_fixture("INVALID", self.file_path)
        self.assertRaises(TypeError, validate_spec, self.file_path)


class ParseSpecTest(unittest.TestCase):
    def setUp(self) -> None:
        self.file_path = pathlib.Path("tests-parse-fixture.json")

    def tearDown(self) -> None:
        os.remove(self.file_path)

    def test_trivial_spec_produces_single_node_dag(self) -> None:
        setup_fixture("TRIVIAL", self.file_path)
        start_node = parse_spec(self.file_path)
        expected_node = Node(name="A", edges=[])
        self.assertEqual(start_node, expected_node)

    def test_simple_spec_produces_three_node_dag(self) -> None:
        setup_fixture("SIMPLE", self.file_path)
        start_node = parse_spec(self.file_path)
        expected_node = Node(
            name="A",
            edges=[
                Edge(Node(name="B", edges=[]), time=0.5),
                Edge(Node(name="C", edges=[]), time=0.7),
            ],
        )
        self.assertIsInstance(start_node, Node)
        self.assertEqual(start_node, expected_node)

    def test_interleaved_spec_produces_four_node_dag(self) -> None:
        setup_fixture("INTERLEAVED", self.file_path)
        start_node = parse_spec(self.file_path)
        expected_node = Node(
            name="A",
            edges=[
                Edge(
                    Node(name="B", edges=[Edge(Node(name="D", edges=[]), time=0.1)]),
                    time=0.4,
                ),
                Edge(Node(name="C", edges=[]), time=0.7),
            ],
        )
        self.assertIsInstance(start_node, Node)
        self.assertEqual(start_node, expected_node)


class RunWorkflowTest(unittest.TestCase):
    LOG_PATTERN = re.compile(r"INFO:traverse\.run_workflow:node=(\w+), timestamp=(.*)")

    def test_trivial_dag(self) -> None:
        node = Node(name="A", edges=[])
        with self.assertLogs("traverse.run_workflow", level="INFO") as cm:
            asyncio.run(run_workflow(node))
            nodes_visited, _ = parse_output(cm.output, self.LOG_PATTERN)
            self.assertListEqual(nodes_visited, ["A"])

    def test_simple_dag(self) -> None:
        node = Node(
            name="A",
            edges=[
                Edge(Node(name="B", edges=[]), time=0.5),
                Edge(Node(name="C", edges=[]), time=0.7),
            ],
        )
        with self.assertLogs("traverse.run_workflow", level="INFO") as cm:
            asyncio.run(run_workflow(node))
            nodes_visited, timings = parse_output(cm.output, self.LOG_PATTERN)
            self.assertListEqual(nodes_visited, ["A", "B", "C"])
            self.assertListEqual(timings, [0.5, 0.7])

    def test_interleaved_dag(self) -> None:
        node = Node(
            name="A",
            edges=[
                Edge(
                    Node(name="B", edges=[Edge(Node(name="D", edges=[]), time=0.1)]),
                    time=0.4,
                ),
                Edge(Node(name="C", edges=[]), time=0.7),
            ],
        )
        with self.assertLogs("traverse.run_workflow", level="INFO") as cm:
            asyncio.run(run_workflow(node))
            nodes_visited, timings = parse_output(cm.output, self.LOG_PATTERN)
            self.assertListEqual(nodes_visited, ["A", "B", "D", "C"])
            self.assertListEqual(timings, [0.4, 0.5, 0.7])
