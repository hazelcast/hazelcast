/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
     * A response indicating the 'null' value.
     */
    static final Object NULL_RESPONSE = new InternalResponse("Invocation::NULL_RESPONSE");

     /**
     * Indicating that there currently is no 'result' available. An example is some kind of blocking
     * operation like ILock.lock. If this lock isn't available at the moment, the wait response
     * is returned.
     */
    static final Object WAIT_RESPONSE = new InternalResponse("Invocation::WAIT_RESPONSE");

    /**
     * A response indicating that a timeout has happened.
     */
    static final Object TIMEOUT_RESPONSE = new InternalResponse("Invocation::TIMEOUT_RESPONSE");

    /**
     * A response indicating that the operation execution was interrupted.
     */
    static final Object INTERRUPTED_RESPONSE = new InternalResponse("Invocation::INTERRUPTED_RESPONSE");

    private String toString;

    InternalResponse(String toString) {
        this.toString = toString;
    }

    @Override
    public String toString() {
        return toString;
    }
}
