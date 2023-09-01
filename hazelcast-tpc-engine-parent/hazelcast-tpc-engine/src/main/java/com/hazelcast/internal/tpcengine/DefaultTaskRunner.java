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

import static com.hazelcast.internal.tpcengine.Task.RUN_COMPLETED;
import static com.hazelcast.internal.tpcengine.util.ExceptionUtil.sneakyThrow;

/**
 * A {@link TaskRunner} that accepts only Runnable or Task instances and runs them.
 * Throws ClassCastException on anything else.
 * <p/>
 * If the task fails to run with an Exception, then the exception is logged. In
 * case of any other throwable, the cause is propagated to the Eventloop and will
 * lead to termination.
 */
public final class DefaultTaskRunner implements TaskRunner {

    public static final DefaultTaskRunner INSTANCE = new DefaultTaskRunner();

    private final TpcLogger logger = TpcLoggerLocator.getLogger(getClass());

    @Override
    public void init(Eventloop eventloop) {
    }

    @Override
    public int run(Object task) throws Throwable {
        if (task instanceof Task) {
            return ((Task) task).run();
        } else {
            ((Runnable) task).run();
            return RUN_COMPLETED;
        }
    }

    @Override
    public int handleError(Object task, Throwable cause) {
        if (cause instanceof Exception) {
            if (logger.isWarningEnabled()) {
                logger.warning("Task " + task + " failed with an exception.", cause);
            }
            return RUN_COMPLETED;
        } else {
            throw sneakyThrow(cause);
        }
    }
}
