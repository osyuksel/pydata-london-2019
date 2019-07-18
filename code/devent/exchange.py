import logging
import random

import pandas as pd
import scipy.stats as st
import simpy

from devent.base import NetworkNode, Packet

# Known constants
REORDER_WINDOW = 50
TRANSMISSION_DELAY = 138


class AlmostFIFOSwitch(NetworkNode):
    def __init__(self, name, env, destination, time_to_destination, delay_dist):
        super().__init__(env, 'switch', name, destination, time_to_destination)
        self.delay_dist = delay_dist

        self.switch_output = simpy.Resource(env, capacity=1)
        self.flush = self.env.event()

        self.processed_packets = simpy.Store(env)
        self.buffer = list()
        self.outgoing_packets = simpy.Store(env)

        self.env.process(self.transmit_packets())
        self.env.process(self.buffer_processed_packets())
        self.env.process(self.flush_buffer())

    @property
    def busy(self):
        return self.switch_output.count == 1

    def process_packet(self, packet):
        t = self.delay_dist.rvs()
        yield self.env.timeout(t)
        self.processed_packets.put(packet)

    def run(self):
        while True:
            packet = yield self.incoming_packets.get()
            self.debug(f'Received {packet}')
            self.env.process(self.process_packet(packet))

    def buffer_processed_packets(self):
        while True:
            packet = yield self.processed_packets.get()
            packet.timestamp(self)
            self.debug(f'Buffering {packet}')
            if len(self.buffer) == 0:
                self.env.process(self.flush_countdown())
            self.buffer.append(packet)

    def flush_countdown(self):
        yield self.env.timeout(REORDER_WINDOW)
        self.debug('Countdown complete')
        self.flush.succeed()

    def flush_buffer(self):
        while True:
            yield self.flush
            self.debug(f'Flushing buffer')

            first = self.buffer[-1]
            random.shuffle(self.buffer)
            if first != self.buffer[0]:
                self.debug(f'First place got overtaken due to reordering. First in: {self.buffer[0]}')

            for p in self.buffer:
                self.outgoing_packets.put(p)
            self.buffer = []
            self.flush = self.env.event()

    def transmit_packets(self):
        while True:
            packet = yield self.outgoing_packets.get()
            with self.switch_output.request() as request:
                self.debug(
                    f'Serving {packet} from queue (size={len(self.outgoing_packets.items)})')
                self.send_to_destination(packet)
                yield self.env.timeout(TRANSMISSION_DELAY)


class BaseSignalGenerator(NetworkNode):
    def __init__(self, name, env, signal_dist):
        super().__init__(env, 'signal', name, None, None)
        self.dist = signal_dist
        self.receivers = []

    def subscribe(self, node):
        store = simpy.Store(self.env, capacity=1)
        self.receivers.append(store)
        self.env.process(node.listen(store))

    def send_signal(self):
        for r in self.receivers:
            r.put((self.name, self.env.now))
        self.debug('Sent out signal.')

    def run(self):
        while True:
            next_signal_time = self.dist.rvs()
            yield self.env.timeout(next_signal_time)
            self.send_signal()


class DependentSignalGenerator(BaseSignalGenerator):
    def __init__(self, name, env, signal_dist, base_signal_generator):
        super().__init__(name, env, signal_dist)
        self.base_signal = base_signal_generator
        self.base_signal.subscribe(self)

    def listen(self, base_signal):
        while True:
            yield base_signal.get()
            self.debug(f'Signal received.')
            next_signal_time = self.dist.rvs()
            yield self.env.timeout(next_signal_time)
            self.send_signal()

    def run(self):
        return NetworkNode.run(self)


class Participant(NetworkNode):
    def __init__(self, name, env, destination, time_to_destination, signal, reaction_time_dist,
                 window_dist):
        super().__init__(env, 'participant', name, destination, time_to_destination)
        signal.subscribe(self)
        self.reaction_time_dist = reaction_time_dist
        self.window_dist = window_dist

    def listen(self, signal):
        while True:
            yield signal.get()
            self.debug(f'Signal received.')
            window = self.window_dist.rvs()

            reaction_time = self.reaction_time_dist.rvs()
            yield self.env.timeout(reaction_time)
            packet = Packet(self)
            packet.metadata = {'window': window,
                               'sending_time': self.env.now,
                               'reaction_time': reaction_time}
            self.send_to_destination(packet)
            self.debug(f'Sent packet to {self.destination}')


class Burst(NetworkNode):
    def __init__(self, name, env, destination, time_to_destination, signal, reaction_time_dist,
                 num_burst_dist):
        super().__init__(env, 'burst', name, destination, time_to_destination)
        signal.subscribe(self)
        self.reaction_time_dist = reaction_time_dist
        self.num_burst_dist = num_burst_dist

    def listen(self, signal):
        while True:
            yield signal.get()
            self.debug(f'Signal received.')
            num_burst = self.num_burst_dist.rvs()
            self.debug(f'Number of bursts: {num_burst}')
            for i in range(0, num_burst):
                self.env.process(self.burst())

    def burst(self):
        reaction_time = self.reaction_time_dist.rvs()
        yield self.env.timeout(reaction_time)
        packet = Packet(self)
        packet.metadata = {'sending_time': self.env.now,
                           'reaction_time': reaction_time}
        self.send_to_destination(packet)
        self.debug(f'Sent packet to {self.destination}')


class MatchingEngine(NetworkNode):
    def __init__(self, name, env):
        super().__init__(env, 'engine', name, None, None)
        self.packets = []

    def run(self):
        while True:
            packet = yield self.incoming_packets.get()
            self.debug(f'Received {packet}. Hops: {packet.hops} \n')
            packet.timestamp(self)
            self.packets.append(packet)


def get_dataframe(packets):
    df_hops = pd.DataFrame([p.hops for p in packets])
    df_meta = pd.DataFrame([p.metadata for p in packets])
    df = df_hops.join(df_meta)
    df['sender'] = [p.sender.name for p in packets]

    df = df.query('sender=="participant"').copy()

    df['time_to_me'] = df['matching_engine'] - df['sending_time']
    df['success'] = (df['reaction_time'] + df['time_to_me']) < df['window']
    return df


def run_simulation(duration, report_interval, reaction_time_mean, num_reactions_mean, burst_reaction_offset, window_loc):
    env = simpy.Environment()
    logger = logging.getLogger(__name__)

    # Known distributions
    signal_dist_prime = st.expon(scale=10 ** 8)
    switch_delay = st.truncnorm(loc=970 - REORDER_WINDOW, scale=50, a=-2, b=2)

    # Parameterized distributions
    reaction_time_dist_imc = st.truncnorm(loc=reaction_time_mean, scale=25, a=-2, b=2)
    num_reactions_dist = st.poisson(mu=num_reactions_mean)
    reaction_time_dist_burst = st.truncnorm(loc=reaction_time_mean + burst_reaction_offset, scale=70, a=-4, b=4)
    window_dist = st.lognorm(loc=window_loc, scale=10, s=7)

    # Nodes
    me = MatchingEngine('matching_engine', env)

    switch_2 = AlmostFIFOSwitch('switch_2', env, me, 1000, switch_delay)

    switch_11 = AlmostFIFOSwitch('switch_11', env, switch_2, 500, switch_delay)
    switch_12 = AlmostFIFOSwitch('switch_12', env, switch_2, 500, switch_delay)

    signal_prime = BaseSignalGenerator('base_signal', env, signal_dist_prime)

    participant = Participant('participant', env, switch_11, 1000, signal_prime, reaction_time_dist_imc, window_dist)

    burst = Burst('burst_1', env, switch_11, 1000, signal_prime, reaction_time_dist_burst, num_reactions_dist)
    burst_2 = Burst('burst_2', env, switch_12, 1000, signal_prime, reaction_time_dist_burst, num_reactions_dist)

    report_interval = int(min(report_interval, duration))
    duration = int(duration)
    for i in range(report_interval, duration + 1, report_interval):
        env.run(i + 1)
        logger.info(f'Simulated {i/1e9:.2f} seconds')

    assert (len(me.packets)>0)
    df = get_dataframe(me.packets)
    logger.info(f'Success rate: {df.success.mean()}')
    return df


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=f"%(levelname)s %(msg)s")
    run_simulation(600 * 1e9, 300 * 1e9, 600, 1, 50, 4500)
