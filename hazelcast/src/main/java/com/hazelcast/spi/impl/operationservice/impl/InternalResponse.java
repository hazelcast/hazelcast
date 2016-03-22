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

package com.hazelcast.spi.impl.operationservice.impl;

final class InternalResponse {

    /**
     * A response indicating that an operation is considered to be dead. So the system has no way of
     * figuring out what happened to the operation or to its response.
     */
    static final Object HEARTBEAT_TIMEOUT = new InternalResponse("Invocation::HEARTBEAT_TIMEOUT");

    /**
     * A response indicating that an operation got rejected on the executing side because its call timeout expired.
     */
    static final Object CALL_TIMEOUT = new InternalResponse("Invocation::CALL_TIMEOUT");

    /**
     * A response indicating that the operation execution was interrupted.
     */
    static final Object INTERRUPTED = new InternalResponse("Invocation::INTERRUPTED");

    static final Object VOID = new InternalResponse("VOID");

    private String toString;

    InternalResponse(String toString) {
        this.toString = toString;
    }

    @Override
    public String toString() {
        return toString;
    }
}
