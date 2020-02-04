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

package com.hazelcast.log;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;

import java.io.Serializable;
import java.util.function.Function;

public class MapTest extends HazelcastTestSupport {

    public void run() {
        HazelcastInstance hz = createHazelcastInstance();
        Log<Trade> tradeLog = hz.getLog("foo");
        for (int k = 0; k < 1024; k++) {
            tradeLog.put(0, new Trade());
        }

        // Log<ComputationResult> computations = tradeLog.map(new Computation(),"computations");
        // now you can access all the computations
    }

    static class Computation implements Function<Trade, ComputationResult> {
        @Override
        public ComputationResult apply(Trade trade) {
            return null;
        }
    }

    static class ComputationResult implements Serializable {

    }

    static class Trade implements Serializable {

    }
}
