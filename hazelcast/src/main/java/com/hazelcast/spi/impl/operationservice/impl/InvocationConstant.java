/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice.impl;

/**
 * Contains some constants for the Invocation to indicate the state of the Invocation.
 */
final class InvocationConstant {

    /**
     * Indicating that an operation is considered to be dead. So the system has no way of
     * figuring out what happened to the operation or to its response.
     */
    static final Object HEARTBEAT_TIMEOUT = new InvocationConstant("Invocation::HEARTBEAT_TIMEOUT");

    /**
     * Indicating that an operation got rejected on the executing side because its call timeout expired.
     */
    static final Object CALL_TIMEOUT = new InvocationConstant("Invocation::CALL_TIMEOUT");

    /**
     * Indicating that the operation execution was interrupted.
     */
    static final Object INTERRUPTED = new InvocationConstant("Invocation::INTERRUPTED");

    /**
     * Indicates that the Invocation has no response yet.
     */
    static final Object VOID = new InvocationConstant("VOID");

    private String toString;

    private InvocationConstant(String toString) {
        this.toString = toString;
    }

    @Override
    public String toString() {
        return toString;
    }
}
