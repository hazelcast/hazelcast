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
import static com.hazelcast.jet.impl.TerminationMode.ActionAfterTerminate.TERMINATE;

public enum TerminationMode {

    // terminate and restart the job
    RESTART_GRACEFUL(true, RESTART, false, JobTerminateRequestedException::new),
    RESTART_FORCEFUL(false, RESTART, false, JobTerminateRequestedException::new),

    // terminate and mark the job as suspended
    SUSPEND_GRACEFUL(true, SUSPEND, false, JobTerminateRequestedException::new),
    SUSPEND_FORCEFUL(false, SUSPEND, false, JobTerminateRequestedException::new),

    // only terminate, don't restart and don't mark it as suspended. Used when
    // master is gracefully shut down.
    TERMINATE_GRACEFUL(true, TERMINATE, false, JobTerminateRequestedException::new),
    TERMINATE_FORCEFUL(false, TERMINATE, false, JobTerminateRequestedException::new),

    // terminate and complete the job
    CANCEL(false, TERMINATE, true, terminationMode -> new CancellationException());

    private final boolean withTerminalSnapshot;
    private final ActionAfterTerminate actionAfterTerminate;
    private final boolean deleteData;
    private final Function<TerminationMode, Exception> exceptionFactory;

    TerminationMode(boolean withTerminalSnapshot, ActionAfterTerminate actionAfterTerminate, boolean deleteData,
                    Function<TerminationMode, Exception> exceptionFactory) {
        this.withTerminalSnapshot = withTerminalSnapshot;
        this.actionAfterTerminate = actionAfterTerminate;
        this.deleteData = deleteData;
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
     * If true, job resources and snapshots should be deleted after
     * termination. Otherwise the job will remain ready to be restarted. It's
     * true only for cancellation.
     */
    public boolean isDeleteData() {
        return deleteData;
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
        } else if (this == TERMINATE_GRACEFUL) {
            res = TERMINATE_FORCEFUL;
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
        /** Don't start the job again, don't mark the job as suspended - used when
         * shutting down a member or cancelling. */
        TERMINATE
    }
}
