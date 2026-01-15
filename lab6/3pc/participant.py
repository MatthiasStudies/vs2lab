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

WORK_CRASH_RATE = 0


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
        self.logger = logging.getLogger("vs2lab.lab6.2pc.Participant")
        self.coordinator = {}
        self.all_participants = {}
        self.state = 'NEW'

    @staticmethod
    def _do_work():
        # Simulate local activities that may succeed or not
        return LOCAL_SUCCESS if random.random() > WORK_CRASH_RATE else LOCAL_ABORT

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



    def run(self):
        # Wait for start of joint commit
        msg = self.channel.receive_from(self.coordinator, TIMEOUT)

        if not msg:  # Crashed coordinator - give up entirely
            # decide to locally abort (before doing anything)
            return "Participant {} terminated in state {} due to {}.".format(
                self.participant, self.state, LOCAL_ABORT)

        assert msg[1] == VOTE_REQUEST

        # Firstly, come to a local decision
        decision = self._do_work()  # proceed with local activities

        # If local decision is negative,
        # then vote for abort and quit directly
        if decision == LOCAL_ABORT:
            self.channel.send_to(self.coordinator, VOTE_ABORT)
            return "Participant {} terminated in state {} due to {}.".format(
                self.participant, self.state, LOCAL_ABORT)

        # If local decision is positive,
        # we are ready to proceed the joint commit

        assert decision == LOCAL_SUCCESS
        self._enter_state('READY')
        # Notify coordinator about local commit vote
        self.channel.send_to(self.coordinator, VOTE_COMMIT)

        # Wait for coordinator to notify the final outcome
        msg = self.channel.receive_from(self.coordinator, TIMEOUT)

        if not msg:  # Crashed coordinator
            # Ask all processes for their decisions
            self.channel.send_to(self.all_participants, NEED_DECISION)
            while True:
                msg = self.channel.receive_from_any()
                # If someone reports a final decision,
                # we locally adjust to it
                if msg[1] in [
                        GLOBAL_COMMIT, GLOBAL_ABORT, LOCAL_ABORT]:
                    decision = msg[1]
                    break

        else:  # Coordinator came to a decision
            decision = msg[1]

        # Change local state based on the outcome of the joint commit protocol
        # Note: If the protocol has blocked due to coordinator crash,
        # we will never reach this point
        if decision in [GLOBAL_ABORT, LOCAL_ABORT]:
            self._enter_state('ABORT')
        elif decision == PREPARE_COMMIT:
            self._enter_state('PRECOMMIT')
            self.channel.send_to(self.coordinator, READY_COMMIT)
        else:
            raise Exception("Unexpected decision: {}".format(decision))

        msg = self.channel.receive_from(self.coordinator, TIMEOUT)

        if not msg:
            # TODO:
            # Ask all processes for their decisions
            pass
        else:
            assert msg[1] == GLOBAL_COMMIT
            decision = GLOBAL_COMMIT
            self._enter_state('COMMIT')

        # Help any other participant when coordinator crashed
        num_of_others = len(self.all_participants) - 1
        while num_of_others > 0:
            num_of_others -= 1
            msg = self.channel.receive_from(self.all_participants, TIMEOUT * 2)
            if msg and msg[1] == NEED_DECISION:
                self.channel.send_to({msg[0]}, decision)

        return "Participant {} terminated in state {} due to {}.".format(
            self.participant, self.state, decision)
