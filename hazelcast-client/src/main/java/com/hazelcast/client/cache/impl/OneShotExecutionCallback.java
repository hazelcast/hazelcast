/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.cache.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.util.Clock;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.hazelcast.util.ExceptionUtil.sneakyThrow;

/**
 * A specific {@link ExecutionCallback} base implementation which has this behaviour:
 * <ul>
 * <li>If it has not been called yet, it will be called and it will be waited to finish.</li>
 * <li>If it has been called but not finished yet, it will be waited to finish.</li>
 * <li>If it has been called and finished already, it will return immediately.</li>
 * </ul>
 *
 * @param <V> type of the response
 */
abstract class OneShotExecutionCallback<V> implements ExecutionCallback<V> {

    private static final AtomicIntegerFieldUpdater CALL_STATE_UPDATER
            = AtomicIntegerFieldUpdater.newUpdater(OneShotExecutionCallback.class, "callState");

    private static final int NOT_CALLED = 0;
    private static final int CALL_IN_PROGRESS = 1;
    private static final int CALL_FINISHED = 2;

    private volatile int callState = NOT_CALLED;

    @Override
    public final void onResponse(V response) {
        onInternal(response, false, Long.MAX_VALUE);
    }

    @Override
    public final void onFailure(Throwable t) {
        onInternal(t, true, Long.MAX_VALUE);
    }

    void onResponse(V response, long finishTime) {
        onInternal(response, false, finishTime);
    }

    void onFailure(Throwable t, long finishTime) {
        onInternal(t, true, finishTime);
    }

    protected abstract void onResponseInternal(V response);

    protected abstract void onFailureInternal(Throwable t);

    private void onInternal(Object obj, boolean failure, long finishTime) {
        if (callState != CALL_FINISHED) {
            // call not finished yet so be sure that it finished by calling it or waiting it to finish
            if (CALL_STATE_UPDATER.compareAndSet(this, NOT_CALLED, CALL_IN_PROGRESS)) {
                // Will be called only by this thread
                try {
                    if (failure) {
                        onFailureInternal((Throwable) obj);
                    } else {
                        onResponseInternal((V) obj);
                    }
                } finally {
                    // call finished
                    callState = CALL_FINISHED;
                }
            } else {
                // call has been started by another thread but not finished yet,
                // so wait until it finished by busy-spin
                while (finishTime > Clock.currentTimeMillis()) {
                    if (callState == CALL_FINISHED) {
                        return;
                    }
                }
                sneakyThrow(new TimeoutException("Waiting for sync execution callback to finish has failed due to timeout!"));
            }
        }
    }
}
