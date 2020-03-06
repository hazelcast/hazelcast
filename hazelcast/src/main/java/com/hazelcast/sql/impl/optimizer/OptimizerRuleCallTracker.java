/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.optimizer;

import java.util.Map;
import java.util.TreeMap;

/**
 * Class which tracks rule invocations in the course of query optimization.
 */
public class OptimizerRuleCallTracker {
    /** Rule calls. */
    private final Map<String, Integer> ruleCalls = new TreeMap<>();

    /** Start time. */
    private long startTime;

    /** Duration. */
    private long duration;

    public void onStart() {
        if (startTime == 0) {
            startTime = System.currentTimeMillis();
        }
    }

    public void onRuleCall(String ruleName) {
        Integer oldCounter = ruleCalls.get(ruleName);
        int newCounter = (oldCounter != null ? oldCounter : 0) + 1;

        ruleCalls.put(ruleName, newCounter);
    }

    public void onDone() {
        if (duration == 0) {
            duration = System.currentTimeMillis() - startTime;
        }
    }

    public Map<String, Integer> getRuleCalls() {
        return ruleCalls;
    }

    public long getDuration() {
        return duration;
    }
}
