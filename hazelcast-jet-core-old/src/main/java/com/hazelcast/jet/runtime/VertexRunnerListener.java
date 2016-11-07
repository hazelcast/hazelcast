/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.runtime;

import com.hazelcast.jet.impl.runtime.ListenerCallable;

/**
 * Listener which will be invoked on change of runner's state
 */
public interface VertexRunnerListener {
    /**
     * Predefined listener to be invoked on runner executed
     */
    ListenerCallable EXECUTED_LISTENER_CALLER = new ListenerCallable() {
        @Override
        public <T extends Throwable> void call(VertexRunnerListener listener, T... payload) {
            listener.onExecuted();
        }
    };

    /**
     * Predefined listener to be invoked on runner interrupted
     */
    ListenerCallable INTERRUPTED_LISTENER_CALLER = new ListenerCallable() {
        @Override
        public <T extends Throwable> void call(VertexRunnerListener listener, T... payload) {
            listener.onExecutionInterrupted();
        }
    };

    /**
     * Predefined listener to be invoked on runner failure
     */
    ListenerCallable FAILURE_LISTENER_CALLER = new ListenerCallable() {
        @Override
        public <T extends Throwable> void call(VertexRunnerListener listener, T... payload) {
            listener.onExecutionFailure(payload[0]);
        }
    };

    /**
     * Will be invoked when runner has been executed
     */
    void onExecuted();

    /**
     * Will be invoked when runner has been interrupted
     */
    void onExecutionInterrupted();

    /**
     * Will be invoked on runner's execution failure
     *
     * @param error the error that was encountered during runner execution
     */
    void onExecutionFailure(Throwable error);
}
