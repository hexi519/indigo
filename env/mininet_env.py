# Copyright 2018 Wei Wang, Yiyang Shao (Huawei Technologies)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.


import sys
import time
from os import path
from subprocess import PIPE, Popen

import context
from dagger.message import Message
from dagger.policy import Policy
from dagger.sender import Sender
from helpers.utils import DEVNULL, check_pid, get_open_port, Config


class MininetEnv(object):
    def __init__(self, env_set, tpg_set, train):
        # traffic pattern group is defined in config.ini and parsed by helper.utils
        self.tpg_set = tpg_set
        self.tpg_set_len = len(self.tpg_set)
        self.tpg_set_idx_env = 0
        self.tpg_set_idx_gen = 0

        self.env_set = env_set
        self.env_set_len = len(self.env_set)
        self.env_set_idx = 0

        self.state_dim = Policy.state_dim
        self.action_cnt = Policy.action_cnt

        self.train = train

        self.done = False

        # variables below will be filled in during setup
        self.sender = None
        self.policy = None
        self.receiver = None
        self.emulator = None
        self.expert_client = None
        self.expert_server = None
        self.perf_client = None
        self.perf_server = None

    def set_expert_client(self, expert_client):
        self.expert_client = expert_client
    
    def set_perf_client(self, perf_client):
        self.perf_client = perf_client

    def set_sample_action(self, sample_action):
        """Set the sender's policy. Must be called before calling reset()."""

        self.sample_action = sample_action

    def is_all_tasks_done(self):
        ret = self.done
        if ret:
            self.done = False
        return ret

    def __update_env_tpg(self):
        if self.tpg_set_idx_gen == len(self.tpg_set[self.tpg_set_idx_env]) - 1:
            # this env is done
            self.env_set_idx = self.env_set_idx + 1
            self.tpg_set_idx_env = self.tpg_set_idx_env + 1
            self.tpg_set_idx_gen = 0
            # all env and traffic shape are done
            if self.env_set_idx == self.env_set_len:
                self.done = True
                self.env_set_idx = 0
                self.tpg_set_idx_env = 0
                self.tpg_set_idx_gen = 0
        else:
            # get next traffic shape for this env
            self.tpg_set_idx_gen = self.tpg_set_idx_gen + 1

    def reset(self):
        """Must be called before running rollout()."""

        self.cleanup()

        self.port = get_open_port()

        # STEP 1: start mininet emulator
        sys.stderr.write('\nStep #1: start mininet emulator: {}\n'
                         .format(' '.join(self.env_set[self.env_set_idx])))
        emulator_path = path.join(context.base_dir, 'env', 'mininet_topo.py')
        cmd = ['python', emulator_path] + self.env_set[self.env_set_idx]
        self.emulator = Popen(cmd, stdin=PIPE, stdout=DEVNULL, stderr=DEVNULL)
        time.sleep(1.5)

        # STEP 2: start traffic generator
        generator = self.tpg_set[self.tpg_set_idx_env][self.tpg_set_idx_gen]
        # use BBR/Cubic/... as the background traffic with iperf/iperf3
        if type(generator) is str and generator.startswith('iperf'):
            sys.stderr.write('Step #2: start {} as traffic generator\n'.format(generator))
            tool, method = generator.split('.')
            param = 'Z' if tool == 'iperf' else 'C'
            self.emulator.stdin.write('h2 {} -s &\n'.format(tool))
            self.emulator.stdin.flush()
            time.sleep(0.1)
            self.emulator.stdin.write(
                'h1 {} -c 192.168.42.2 -{} {} -M {} &\n'.format(tool, param, method, Message.total_size))
            self.emulator.stdin.flush()
        else:
            sys.stderr.write('Step #2: start traffic generator: ')
            # tg-receiver:
            tg_receiver_path = path.join(
                context.base_dir, 'traffic-generator', 'receiver.py')
            self.emulator.stdin.write(
                'h2 python ' + tg_receiver_path + ' 192.168.42.2 6666 &\n')
            self.emulator.stdin.flush()
            time.sleep(0.1)

            # tg-sender: parameters: ip port NIC -s [sketch] -c cycle -l lifetime
            sketch, cycle = generator
            sys.stderr.write('{}\n'.format(sketch))
            # one_way_delay = convert_to_seconds(self.env_set[self.env_set_idx][1])
            # lifetime = Policy.steps_per_episode * one_way_delay * 2 / Policy.action_frequency * 1.5
            lifetime = 1000  # 0 stands for run forever
            tg_sender_path = path.join(
                context.base_dir, 'traffic-generator', 'sender.py')
            self.emulator.stdin.write('h1 python {} 192.168.42.2 6666 h1-eth0 -s "{}" -c {} -l {} &\n'
                                      .format(tg_sender_path, sketch, cycle, lifetime))
            self.emulator.stdin.flush()
            time.sleep(0.1)

        # STEP 3: start receiver
        sys.stderr.write('Step #3: start receiver\n')
        time.sleep(0.1)
        receiver_path = path.join(context.base_dir, 'dagger', 'receiver.py')
        self.emulator.stdin.write('h3 python ' + receiver_path + ' ' + str(self.port) + ' &\n')
        self.emulator.stdin.flush()

        # STEP 4: start expert server or perf server in train or test mode
        if self.train:
            expert_server_path = path.join(
                context.base_dir, 'dagger', 'expert_server.py')
            # try 3 times at most to ensure expert server is started normally
            for i in xrange(3):
                self.expert_server_port = get_open_port()
                cmd = ['python', expert_server_path, str(self.expert_server_port)]
                self.expert_server = Popen(cmd)  # , stdout=DEVNULL, stderr=DEVNULL
                if check_pid(self.expert_server.pid):
                    sys.stderr.write(
                        'Step #4: start expert server (PID: {}), '.format(
                            self.expert_server.pid))
                    break
                else:
                    sys.stderr.write(
                        'start expert server failed, try again...\n')
                    self.__update_env_tpg()
                    return -1
        elif Config.measurement:
            perf_server_path = path.join(context.base_dir, 'dagger', 'perf_server.py')
            # try 3 times at most to ensure perf server is started normally
            for i in xrange(3):
                self.perf_server_port = get_open_port()
                cmd = ['python', perf_server_path, str(self.perf_server_port),
                       self.env_set_idx, self.tpg_set_idx_env, self.tpg_set_idx_gen]
                self.perf_server = Popen(cmd, stdin=DEVNULL,
                                           stdout=DEVNULL, stderr=DEVNULL)
                if check_pid(self.perf_server.pid):
                    sys.stderr.write('Step #4: start perf server successfully\n')
                    break
                else:
                    sys.stderr.write('\nstart perf server failed, try again...\n')
                    self.__update_env_tpg()
                    return -1
        time.sleep(1)
        if self.train:
            if self.expert_client == None:
                sys.stderr.write('\nNo expert_client set\n')
                self.__update_env_tpg()
                return -1
            ret = -1
            for i in xrange(3):
                ret = self.expert_client.connect_expert_server(self.expert_server_port)
                if ret == 0:
                    sys.stderr.write('connect to expert server successfully\n')
                    break
                time.sleep(0.5)
            if ret == -1:
                sys.stderr.write('connect to expert server failed\n')
                self.__update_env_tpg()
                return -1
        elif Config.measurement:
            if self.perf_client == None:
                sys.stderr.write('\nNo perf_client set\n')
                self.__update_env_tpg()
                return -1
            for i in xrange(3):
                ret = self.perf_client.connect_perf_server(self.perf_server_port)
                if ret == 0:
                    sys.stderr.write('connect to perf server successfully\n')
                    break
                time.sleep(0.5)
            if ret == -1:
                sys.stderr.write('connect to perf server failed\n')
                self.__update_env_tpg()
                return -1

        # STEP 5: start sender
        sys.stderr.write('Step #5: start sender\n')
        # create a policy instance
        self.policy = Policy(self.train)
        self.policy.set_sample_action(self.sample_action)
        # create a sender instance
        self.sender = Sender('192.168.42.222', self.port)
        self.sender.set_policy(self.policy)
        if not self.train:
            self.sender.set_run_time(Config.run_time * 1000) # ms

        # Update env and corresponding traffic shape in this episode
        self.__update_env_tpg()
        sys.stderr.write('env reset done\n')
        return 0

    def rollout(self):
        """Run sender in env, get final reward of an episode, reset sender."""

        sys.stderr.write('Obtaining an episode from environment...\n')
        ret = self.sender.run()
        return ret

    def cleanup(self):
        if self.expert_client:
            self.expert_client.cleanup()
            # can not set to None
        
        if self.perf_client:
            self.perf_client.cleanup()
            # can not set to None

        if self.expert_server:
            Popen('pkill -f expert_server', shell=True)
            self.expert_server = None
        
        if self.perf_server:
            Popen('pkill -f perf_server', shell=True)
            self.perf_server = None

        if self.emulator:
            # stop tg-sender, tg-receiver, receiver
            self.emulator.stdin.write('h1 pkill -f sender\n')
            self.emulator.stdin.write('h2 pkill -f receiver\n')
            self.emulator.stdin.write('h3 pkill -f receiver\n')
            time.sleep(0.5)
            # stop emulator
            self.emulator.stdin.write('quit()\n')
            time.sleep(3)  # wait for the mininet to be closed completely
            self.emulator = None

        if self.sender:
            self.sender.cleanup()
            self.sender = None