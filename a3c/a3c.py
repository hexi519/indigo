import sys
import numpy as np
import tensorflow as tf
from tensorflow.contrib import layers


class ActorCriticNetwork(object):
    def __init__(self, state_dim, action_cnt):
        self.states = tf.placeholder(tf.float32, [None, state_dim])

        actor_h1 = layers.relu(self.states, 10)
        self.action_scores = layers.linear(actor_h1, action_cnt)

        logits = self.action_scores - tf.reduce_max(
            self.action_scores, [1], keep_dims=True)
        self.predicted_actions = tf.reshape(tf.multinomial(logits, 1), [-1])

        critic_h1 = layers.relu(self.states, 10)
        self.state_values = tf.reshape(layers.linear(critic_h1, 1), [-1])

        self.trainable_vars = tf.get_collection(
            tf.GraphKeys.TRAINABLE_VARIABLES, tf.get_variable_scope().name)


class A3C(object):
    def __init__(self, cluster, server, worker_device, env):
        # distributed tensorflow related
        self.cluster = cluster
        self.server = server
        self.worker_device = worker_device
        self.env = env

        self.state_dim = env.state_dim
        self.action_cnt = env.action_cnt

        self.max_global_episode = 2000

        # must call env.set_sample_action() before env.run()
        env.set_sample_action(self.sample_action)

        # start tensorflow session and build tensorflow graph
        self.session = tf.Session(server.target)
        self.build_tf_graph()

        # initialize variables
        self.session.run(tf.global_variables_initializer())

    def cleanup(self):
        self.env.cleanup()

    def build_tf_graph(self):
        with tf.device(tf.train.replica_device_setter(
                worker_device=self.worker_device,
                cluster=self.cluster)):
            with tf.variable_scope('global'):
                self.global_network = ActorCriticNetwork(
                    state_dim=self.state_dim, action_cnt=self.action_cnt)
                self.global_episode = tf.get_variable(
                    'global_episode', [], tf.int32,
                    initializer=tf.constant_initializer(0, tf.int32),
                    trainable=False)

        with tf.device(self.worker_device):
            with tf.variable_scope('local'):
                self.local_network = ActorCriticNetwork(
                    state_dim=self.state_dim, action_cnt=self.action_cnt)

            self.build_loss()

    def build_loss(self):
        pi = self.local_network

        self.states = pi.states
        self.actions = tf.placeholder(tf.int32, [None])
        self.rewards = tf.placeholder(tf.float32, [None])
        self.advantages = self.rewards - pi.state_values

        # policy loss
        cross_entropy_loss = tf.nn.sparse_softmax_cross_entropy_with_logits(
            logits=pi.action_scores, labels=self.actions)
        policy_loss = tf.reduce_mean(cross_entropy_loss * self.advantages)

        # value loss
        value_loss = 0.5 * tf.reduce_mean(tf.square(self.advantages))

        # add entropy to loss to encourage exploration
        action_probs = tf.nn.softmax(pi.action_scores)
        log_action_probs = tf.nn.log_softmax(pi.action_scores)
        entropy = -tf.reduce_mean(action_probs * log_action_probs)

        # total loss and gradients
        loss = policy_loss + 0.5 * value_loss - 0.01 * entropy
        grads = tf.gradients(loss, pi.trainable_vars)
        grads, _ = tf.clip_by_global_norm(grads, 40.0)

        # sync local network to global network
        self.sync_op = tf.group(*[v1.assign(v2) for v1, v2 in zip(
            pi.trainable_vars, self.global_network.trainable_vars)])

        # calculate gradients and apply to global network
        grads_and_vars = list(zip(grads, self.global_network.trainable_vars))
        inc_global_episode = self.global_episode.assign_add(1)

        optimizer = tf.train.AdamOptimizer(1e-4)
        self.train_op = tf.group(
            optimizer.apply_gradients(grads_and_vars), inc_global_episode)

    def sample_action(self, state):
        norm_state = self.normalize_states([state])

        return self.session.run(self.local_network.predicted_actions[0],
                                {self.local_network.states: norm_state})

    def normalize_states(self, states):
        norm_states = np.array(states, dtype=np.float32)

        # queuing_delay, mostly in [0, 210]
        queuing_delays = norm_states[:, 0]
        queuing_delays /= 105.0
        queuing_delays -= 1.0

        # send_ewma and ack_ewma, mostly in [0, 32]
        for i in [1, 2]:
            ewmas = norm_states[:, i]
            ewmas /= 16.0
            ewmas -= 1.0

        # make sure all features lie in [-1.0, 1.0]
        norm_states[norm_states > 1.0] = 1.0
        norm_states[norm_states < -1.0] = -1.0
        return norm_states

    def discount_rewards(self, final_reward, episode_len):
        return [final_reward] * episode_len

    def run(self):
        global_episode = self.session.run(self.global_episode)

        while global_episode < self.max_global_episode:
            sys.stderr.write('Global Episode: %d\n' % global_episode)

            # reset local parameters to global
            self.session.run(self.sync_op)

            # get an episode of experience
            self.env.run()
            state_buf, action_buf, final_reward = self.env.get_experience()
            self.env.reset()

            # process the episode
            state_buf = self.normalize_states(state_buf)
            reward_buf = self.discount_rewards(final_reward, len(action_buf))

            # train using the episode
            self.session.run(self.train_op, {
                self.states: state_buf,
                self.actions: action_buf,
                self.rewards: reward_buf,
            })

            global_episode = self.session.run(self.global_episode)
