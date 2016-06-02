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

package com.hazelcast.jet.api.container;

import com.hazelcast.jet.internal.api.container.ContainerListenerCaller;

/**
 * Listener which will be invoked on change of container's state;
 */
public interface ContainerListener {
    /**
     * Predefined listener to be invoked on container executed
     */
    ContainerListenerCaller EXECUTED_LISTENER_CALLER = new ContainerListenerCaller() {
        @Override
        public <T extends Throwable> void call(ContainerListener listener, T... payLoad) {
            listener.onContainerExecuted();
        }
    };

    /**
     * Predefined listener to be invoked on container interrupted
     */
    ContainerListenerCaller INTERRUPTED_LISTENER_CALLER = new ContainerListenerCaller() {
        @Override
        public <T extends Throwable> void call(ContainerListener listener, T... payLoad) {
            listener.onContainerExecutionInterrupted();
        }
    };

    /**
     * Predefined listener to be invoked on container failure
     */
    ContainerListenerCaller FAILURE_LISTENER_CALLER = new ContainerListenerCaller() {
        @Override
        public <T extends Throwable> void call(ContainerListener listener, T... payLoad) {
            listener.onContainerExecutionFailure(payLoad[0]);
        }
    };

    /**
     * Will be invoked when container has been executed;
     */
    void onContainerExecuted();

    /**
     * Will be invoked when container has been interrupted;
     */
    void onContainerExecutionInterrupted();

    /**
     * Will be invoked on container's execution failure;
     *
     * @param error the error that was encountered during container execution
     */
    void onContainerExecutionFailure(Throwable error);
}
