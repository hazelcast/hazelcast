/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.kinesis.impl.sink;

import javax.annotation.Nonnull;

/**
 * A simple state machine, based on an ordered list of states. It
 * handles two types of events, successes, and errors. On a success, it
 * transitions from the current state to the one on its left (smaller
 * index); on error, it transitions from the current state to one on its
 * right (bigger index). It's asymmetrical in that on success it changes
 * the state index to the left by 1, while on error it changes the state
 * index to the right by a larger number, let's call it K.
 * <p>
 * One concrete use is to track sleep times, which get increased by a
 * significant amount on error and only decrease gradually, much slower
 * than their increase, on successes.
 * <p>
 * We are attaching an output to each of the states defined. First K
 * states have the same output, next K states have a different output,
 * and so on. Using the above example of the sleep times, think of the
 * output as the actual sleep time. So when an error happens, the state
 * change should be big enough so that the output changes. When
 * successes happen, it should take multiple of them to change the
 * output in the opposite direction.
 */
class SlowRecoveryDegrader<T> {

    private final int k;
    private final T[] outputs;
    private final int n;

    private int state; //[0 .. k*n]

    SlowRecoveryDegrader(int k, T[] outputs) {
        if (k < 1) {
            throw new IllegalArgumentException("Speed ratio can't be smaller than 1");
        }
        this.k = k;

        if (outputs == null || outputs.length < 2) {
            throw new IllegalArgumentException("At least two outputs needed");
        }
        for (T output : outputs) {
            if (output == null) {
                throw new IllegalArgumentException("Output can't be null");
            }
        }
        this.outputs = outputs;
        this.n = outputs.length - 1;
    }

    void ok() {
        if (state > 0) {
            state--;
        }
    }

    void error() {
        if (state <= k * (n - 1)) {
            state += k;
        } else {
            state = k * n;
        }
    }

    @Nonnull
    T output() {
        return outputs[state % k == 0 ? state / k : 1 + state / k];
    }

}
