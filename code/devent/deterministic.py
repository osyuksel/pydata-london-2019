import logging
import simpy

from devent.base import NetworkNode, Packet

SWITCH_PROCESSING_DELAY = 970
SWITCH_TO_MATCHING_ENGINE = 1500
SIGNAL_PERIOD = 10 ** 8
REACTION_TIME = 500
TIME_TO_SWITCH = 1000
TRANSMISSION_DELAY = 180


class FIFOSwitch(NetworkNode):
    def __init__(self, name, env, destination, time_to_destination):
        super().__init__(env, 'switch', name, destination, time_to_destination)
        self.busy = False

        self.env.process(self.transmit_outgoing_packets())
        self.outgoing_packets = simpy.Store(env)

    def run(self):
        while True:
            packet = yield self.incoming_packets.get()
            self.debug(f'{packet} arrived')
            self.env.process(self.process_packet(packet))

    def process_packet(self, packet):
        yield self.env.timeout(SWITCH_PROCESSING_DELAY)
        self.outgoing_packets.put(packet)

    def transmit_outgoing_packets(self):
        while True:
            packet = yield self.outgoing_packets.get()
            self.debug(
                f'Transmitting packet. Remaining #packets in queue: {len(self.outgoing_packets.items)} ')
            self.send_to_destination(packet)
            yield (self.env.timeout(TRANSMISSION_DELAY))


class SimpleParticipant(NetworkNode):
    def __init__(self, name, env, destination, time_to_destination):
        super().__init__(env, 'participant', name, destination, time_to_destination)

    def run(self):
        while True:
            yield self.env.timeout(SIGNAL_PERIOD)
            self.debug(f'Signal received.')

            yield self.env.timeout(REACTION_TIME)
            self.debug(f'Sent packet to {self.destination}')

            self.send_to_destination(Packet(self))


class MatchingEngine(NetworkNode):
    def __init__(self, name, env):
        super().__init__(env, 'engine', name, None, None)

    def run(self):
        while True:
            packet = yield self.incoming_packets.get()
            self.debug(f'Received {packet}.')


def run_simulation(duration):
    env = simpy.Environment()
    logging.basicConfig(level=logging.DEBUG, format=f"%(levelname)s %(msg)s")

    # Nodes
    matching_engine = MatchingEngine('matching_engine', env)
    switch = FIFOSwitch('Switch1', env, matching_engine, SWITCH_TO_MATCHING_ENGINE)
    alice = SimpleParticipant('Alice', env, switch, TIME_TO_SWITCH)
    bob = SimpleParticipant('Bob', env, switch, TIME_TO_SWITCH)

    env.run(duration)


if __name__ == '__main__':
    run_simulation(1.2e8)
