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
 * For streaming jobs it always add {@link QueryEndException} - even if query is marked as done normally. That's because
 * streaming queries should never end, so context closing via exception is nothing suspicious. We still
 * hide this exception on the client side.
 *
 * Note that it remembers only the first exception that is passed.
 */
class QueryEndTracker {

    private final boolean isBatch;
    private volatile boolean done;
    private final AtomicReference<Exception> exception = new AtomicReference<>();

    QueryEndTracker(boolean isBatch) {
        this.isBatch = isBatch;
    }

    void markDone() {
        if (!isBatch) {
            // exception just to make the streaming job end
            exception.compareAndSet(null, new QueryEndException());
        }
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
            return ex == null || ex instanceof QueryEndException
                    ? Status.DONE_NORMALLY
                    : Status.DONE_EXCEPTIONALLY;
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

    /**
     * Exception marking the query as completed even if the inbound edges are still producing values.
     *
     * Needed for streaming jobs, where {@link com.hazelcast.jet.core.Processor#complete} will never be called
     * if inbound edges are too fast. Should be always ignored on client side, it just means "no more data, but it's ok".
     */
    private static class QueryEndException extends RuntimeException {

        private QueryEndException() {
            // Use writableStackTrace = false, the exception is not created at a place where it's thrown,
            // it's better if it has no stack trace then.
            super("Done by reaching the end specified by the query", null, false, false);
        }
    }
}
