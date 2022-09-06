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

package com.hazelcast.jet.sql.impl;

import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

/**
 * Tracks if execution was already done. There can be two possible ways of marking things as done:
 * - normally, without exception
 * - exceptionally, when some exception is being thrown.
 *
 * Note that it remembers only the first exception that is passed.
 */
class DoneTracker {

    private volatile boolean done;
    private final AtomicReference<Exception> exception = new AtomicReference<>();

    void markDone() {
        done = true;
    }

    void markDone(Exception ex) {
        exception.compareAndSet(null, ex);
        done = true;
    }

    boolean isDone() {
        return done;
    }

    void ensureNotDoneExceptionally() {
        Exception ex = exception.get();
        if (ex != null) {
            throw sneakyThrow(ex);
        }
    }

    Status status() {
        if (done) {
            Exception ex = exception.get();
            return ex == null ? Status.DONE_NORMALLY : Status.DONE_EXCEPTIONALLY;
        } else {
            return Status.NOT_DONE;
        }
    }

    Exception exception() {
        return exception.get();
    }

    enum Status {
        DONE_NORMALLY,
        DONE_EXCEPTIONALLY,
        NOT_DONE
    }

}
