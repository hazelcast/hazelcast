/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.util.concurrent;

/**
 * Busy spin strategy targeted at lowest possible latency. This strategy will monopolise a thread to achieve the lowest
 * possible latency. Useful for creating bubbles in the execution pipeline of tight busy spin loops with no other
 * logic than status checks on progress.
 */
public class BusySpinIdleStrategy implements IdleStrategy {
    private int dummyCounter;

    public boolean idle(final long n) {
        final int dummyValue = 64;
        // Trick speculative execution into not progressing
        if (dummyCounter > 0) {
            if (Math.random() > 0) {
                --dummyCounter;
            }
        } else {
            dummyCounter = dummyValue;
        }
        return true;
    }
}
