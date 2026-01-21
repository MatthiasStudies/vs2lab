import random
import logging

# coordinator messages
from const3PC import VOTE_REQUEST, GLOBAL_COMMIT, GLOBAL_ABORT, PREPARE_COMMIT
# participant decissions
from const3PC import LOCAL_SUCCESS, LOCAL_ABORT, READY_COMMIT
# participant messages
from const3PC import VOTE_COMMIT, VOTE_ABORT, NEED_DECISION
# misc constants
from const3PC import TIMEOUT

import stablelog

# WORK_CRASH_RATE = 1/3

WORK_CRASH_RATE = 1/3


class Participant:
    """
    Implements a two phase commit participant.
    - state written to stable log (but recovery is not considered)
    - in case of coordinator crash, participants mutually synchronize states
    - system blocks if all participants vote commit and coordinator crashes
    - allows for partially synchronous behavior with fail-noisy crashes
    """

    def __init__(self, chan):
        self.channel = chan
        self.participant = self.channel.join('participant')
        self.stable_log = stablelog.create_log(
            "participant-" + self.participant)
        self.logger = logging.getLogger("vs2lab.lab6.3pc.Participant")
        self.coordinator = {}
        self.all_participants: set = set()
        self.state = 'NEW'

    @staticmethod
    def _do_work():
        # Simulate local activities that may succeed or not
        return random.random() > WORK_CRASH_RATE

    def _enter_state(self, state):
        self.stable_log.info(state)  # Write to recoverable persistant log file
        self.logger.info("Participant {} entered state {}."
                         .format(self.participant, state))
        self.state = state

    def init(self):
        self.channel.bind(self.participant)
        self.coordinator = self.channel.subgroup('coordinator')
        self.all_participants = self.channel.subgroup('participant')
        self._enter_state('INIT')  # Start in local INIT state.

    def _get_new_coordinator(self):
        participants = sorted(self.all_participants)
        return participants[0]

    def _become_new_coordinator(self):
        self.logger.info("[Participant {}] I am the new coordinator.".format(
            self.participant))

        decision = None

        if self.state == 'READY':
            self._enter_state('ABORT')
            decision = GLOBAL_ABORT
        elif self.state == 'PRECOMMIT':
            self._enter_state('COMMIT')
            decision = GLOBAL_COMMIT
        elif self.state == 'COMMIT':
            decision = GLOBAL_COMMIT
        elif self.state == 'ABORT':
            decision = GLOBAL_ABORT
        else:
            self._enter_state('ABORT')
            decision = GLOBAL_ABORT

        self.logger.info("[Participant {}] New coordinator decided {}.".format(
            self.participant, decision))

        self.channel.send_to(self.all_participants, decision)

        return "Participant {} terminated in state {} due to {}.".format(
            self.participant, self.state, decision)

    def on_coordinator_timeout(self):
        self.logger.info("[Participant {}] Coordinator timeout in state {}".format(self.participant, self.state))

        if self.state == 'INIT':
            self._enter_state('ABORT')
            return "Participant {} terminated in state {} due to {}.".format(
                self.participant, self.state, LOCAL_ABORT)

        while True:
            new_coordinator = self._get_new_coordinator()
            self.all_participants.remove(new_coordinator)

            if new_coordinator == self.participant:
                return self._become_new_coordinator()

            self.logger.info("[Participant {}] Asking new coordinator {} for decision.".format(
                self.participant, new_coordinator))
            msg = self.channel.receive_from([new_coordinator], TIMEOUT)
            if not msg:
                continue

            if msg[1] == GLOBAL_COMMIT:
                self._enter_state('COMMIT')
                return "Participant {} terminated in state {} due to {}.".format(
                    self.participant, self.state, GLOBAL_COMMIT)
            elif msg[1] == GLOBAL_ABORT:
                self._enter_state('ABORT')
                return "Participant {} terminated in state {} due to {}.".format(
                    self.participant, self.state, GLOBAL_ABORT)




    def run(self):
        msg = self.channel.receive_from(self.coordinator, TIMEOUT)
        if not msg:
            return self.on_coordinator_timeout()

        assert msg[1] == VOTE_REQUEST

        success = self._do_work()
        if success:
            self._enter_state('READY')
            self.channel.send_to(self.coordinator, VOTE_COMMIT)
        else:
            self._enter_state('ABORT')
            self.channel.send_to(self.coordinator, VOTE_ABORT)
            return "Participant {} terminated in state {} due to {}.".format(
                self.participant, self.state, LOCAL_ABORT)

        assert self.state == 'READY'

        msg = self.channel.receive_from(self.coordinator, TIMEOUT)
        if not msg:
            return self.on_coordinator_timeout()

        if msg[1] == GLOBAL_ABORT:
            self._enter_state('ABORT')
            return "Participant {} terminated in state {} due to {}.".format(
                self.participant, self.state, GLOBAL_ABORT)
        else:
            assert msg[1] == PREPARE_COMMIT
            self._enter_state('PRECOMMIT')
            self.channel.send_to(self.coordinator, READY_COMMIT)

        assert self.state == 'PRECOMMIT'

        msg = self.channel.receive_from(self.coordinator, TIMEOUT)
        if not msg:
            return self.on_coordinator_timeout()

        if msg[1] == GLOBAL_ABORT:
            self._enter_state('ABORT')
            return "Participant {} terminated in state {} due to {}.".format(
                self.participant, self.state, GLOBAL_ABORT)
        else:
            assert msg[1] == GLOBAL_COMMIT
            self._enter_state('COMMIT')
            return "Participant {} terminated in state {} due to {}.".format(
                self.participant, self.state, GLOBAL_COMMIT)
