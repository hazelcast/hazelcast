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

package com.hazelcast.jet.impl;

import com.hazelcast.jet.impl.exception.JobTerminateRequestedException;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.util.concurrent.CancellationException;
import java.util.function.Function;

import static com.hazelcast.jet.impl.TerminationMode.ActionAfterTerminate.RESTART;
import static com.hazelcast.jet.impl.TerminationMode.ActionAfterTerminate.SUSPEND;

public enum TerminationMode {

    // terminate and restart the job
    RESTART_GRACEFUL(true, RESTART, JobTerminateRequestedException::new),
    RESTART_FORCEFUL(false, RESTART, JobTerminateRequestedException::new),

    // terminate and mark the job as suspended
    SUSPEND_GRACEFUL(true, SUSPEND, JobTerminateRequestedException::new),
    SUSPEND_FORCEFUL(false, SUSPEND, JobTerminateRequestedException::new),

    // terminate and complete the job
    CANCEL(false, ActionAfterTerminate.NO_ACTION, terminationMode -> new CancellationException());

    private final boolean withTerminalSnapshot;
    private final ActionAfterTerminate actionAfterTerminate;
    private final Function<TerminationMode, Exception> exceptionFactory;

    TerminationMode(boolean withTerminalSnapshot, ActionAfterTerminate actionAfterTerminate,
                    Function<TerminationMode, Exception> exceptionFactory) {
        this.withTerminalSnapshot = withTerminalSnapshot;
        this.actionAfterTerminate = actionAfterTerminate;
        this.exceptionFactory = exceptionFactory;
    }

    /**
     * If true, the job should be terminated with a terminal snapshot. If
     * false, it should be interrupted.
     */
    public boolean isWithTerminalSnapshot() {
        return withTerminalSnapshot;
    }

    /**
     * Returns the action that should be done after the job terminates.
     */
    public ActionAfterTerminate actionAfterTerminate() {
        return actionAfterTerminate;
    }

    /**
     * Returns a copy of this TerminationMode with terminal snapshot disabled.
     */
    @CheckReturnValue
    public TerminationMode withoutTerminalSnapshot() {
        TerminationMode res = this;
        if (this == SUSPEND_GRACEFUL) {
            res = SUSPEND_FORCEFUL;
        } else if (this == RESTART_GRACEFUL) {
            res = RESTART_FORCEFUL;
        }
        assert !res.isWithTerminalSnapshot() : "mode still has (withTerminalSnapshot == true): " + res;
        return res;
    }

    @Nonnull
    public Exception createException() {
        return exceptionFactory.apply(this);
    }

    public enum ActionAfterTerminate {
        /** Start the job again. */
        RESTART,
        /** Don't start the job again, mark the job as suspended. */
        SUSPEND,
        /** Used when cancelling. */
        NO_ACTION
    }
}
