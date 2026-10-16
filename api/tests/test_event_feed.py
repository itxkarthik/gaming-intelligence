"""Kafka topic discovery behavior for the shared live event feed."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from kafka import TopicPartition

from state import EventFeed


class TopicDiscoveryConsumer:
    def __init__(self):
        self.assignments = []
        self.seeks = []

    def partitions_for_topic(self, topic):
        return {"gameplay": None, "players": {0, 1}}.get(topic)

    def assign(self, partitions):
        self.assignments.append(partitions)

    def end_offsets(self, partitions):
        return {partition: 100 for partition in partitions}

    def seek(self, partition, offset):
        self.seeks.append((partition, offset))


class EventFeedTests(unittest.TestCase):
    def test_assign_new_skips_missing_topics_and_assigns_available_partitions(self):
        feed = EventFeed(topics=("gameplay", "players"))
        consumer = TopicDiscoveryConsumer()

        assigned = feed._assign_new(consumer, set())

        expected = {TopicPartition("players", 0), TopicPartition("players", 1)}
        self.assertEqual(assigned, expected)
        self.assertEqual(consumer.assignments, [sorted(expected)])
        self.assertEqual(consumer.seeks, [(TopicPartition("players", 0), 75),
                                          (TopicPartition("players", 1), 75)])
