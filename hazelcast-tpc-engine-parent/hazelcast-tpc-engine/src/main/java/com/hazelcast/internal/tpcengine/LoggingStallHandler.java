/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine;

import com.hazelcast.internal.tpcengine.logging.TpcLogger;
import com.hazelcast.internal.tpcengine.logging.TpcLoggerLocator;

/**
 * A {@link StallHandler} that writes a log entry when a stall is detected.
 * <p/>
 * This is not a great implementation because it can really spam the log files
 * with violations. It is much better to have one that can aggregate.
 */
public final class LoggingStallHandler implements StallHandler {

    public static final LoggingStallHandler INSTANCE = new LoggingStallHandler();

    private final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());

    @Override
    public void onStall(Reactor reactor, TaskQueue taskQueue, Object task,
                        long startNanos, long durationNanos) {
        if (logger.isSevereEnabled()) {
            logger.severe(reactor + " detected stall of " + durationNanos
                    + " ns, the culprit is " + task
                    + " in taskQueue " + taskQueue.name);
        }
    }
}
