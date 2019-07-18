import logging
from collections import OrderedDict
from dataclasses import dataclass, field
import simpy


class NetworkNode(object):
    """
    An abstract class representing a network node, e.g. switch, participant, matching engine.

    """

    def __init__(self, env, class_label, name, destination, time_to_destination):
        self.env = env
        self.destination = destination
        self.time_to_destination = time_to_destination
        self.name = name
        self.class_label = class_label
        self.incoming_packets = simpy.Store(env)
        self.logger = logging.getLogger(self.name)

        self.env.process(self.run())

    def __repr__(self):
        return f'{self.class_label} ({self.name})'

    def run(self):
        while True:
            yield self.incoming_packets.get()

    def packet_in_flight(self, value, delay):
        yield self.env.timeout(delay)
        self.destination.incoming_packets.put(value)

    def send_to_destination(self, value):
        self.env.process(self.packet_in_flight(value, self.time_to_destination))

    def debug(self, msg):
        self.logger.debug(f'[{int(self.env.now)}] {self.name} - {msg}')


@dataclass
class Packet:
    """
    A network packet sent by a participant. It also contains metadata and timestamps from the nodes on its path.
    """

    env: simpy.Environment = field(init=False)
    sender: NetworkNode
    hops: OrderedDict = field(init=False, default_factory=OrderedDict)
    metadata: dict = field(init=False, default_factory=dict)

    def __post_init__(self):
        self.env = self.sender.env

    def timestamp(self, node):
        self.hops[node.name] = int(self.env.now)

    def __repr__(self):
        return f'Packet ({self.sender.name})'
