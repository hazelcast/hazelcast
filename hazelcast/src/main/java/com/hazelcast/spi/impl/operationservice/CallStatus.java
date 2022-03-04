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

package com.hazelcast.spi.impl.operationservice;

/**
 * The result of an {@link Operation#call()}.
 *
 * Using the CallStatus the operation can control how the system will deal with
 * the operation after it is executed. For example when the CallStatus is
 * {@link CallStatus#RESPONSE}, a response can be send to the caller. But
 * it also allows for different behavior where no response is available yet e.g.
 * when an operation gets offloaded.
 *
 * <h1>Future additions</h1>
 * In the future we can add more values to this enumeration, for example 'YIELD'
 * for batching operations that wants to release the operation thread so that
 * other operations can be interleaved.
 */
public class CallStatus {

    /**
     * The ordinal value for a {@link #RESPONSE}.
     */
    public static final int RESPONSE_ORDINAL = 0;

    /**
     * The ordinal value for a {@link #VOID}.
     */
    public static final int VOID_ORDINAL = 1;

    /**
     * The ordinal value for a {@link #WAIT}.
     */
    public static final int WAIT_ORDINAL = 2;

    /**
     * The ordinal value for an {@link Offload}.
     */
    public static final int OFFLOAD_ORDINAL = 3;

    /**
     * Signals that the Operation is done running and that a response is ready
     * to be returned. Most of the normal operations like IAtomicLong.get will
     * fall in this category.
     *
     * Also operations like an IMap.put should return 'RESPONSE' because the
     * operation will be checked if backups should be made.
     */
    public static final CallStatus RESPONSE = new CallStatus(RESPONSE_ORDINAL);

    /**
     * Signals that the Operation is done running, but no response will be
     * returned. Most of the regular operations like map.get will return a
     * response, but there are also fire and forget operations (lot of
     * cluster operations) that don't return a response.
     *
     * If an operation returns void, it will be checked if backups should be made.
     */
    public static final CallStatus VOID = new CallStatus(VOID_ORDINAL);

    /**
     * Indicates that the call could not complete because waiting is required.
     * E.g. a queue.take on an empty queue. This can only be returned by
     * BlockingOperations.
     *
     * In this case no responses/backups will be send.
     */
    public static final CallStatus WAIT = new CallStatus(WAIT_ORDINAL);

    private final int ordinal;

    protected CallStatus(int ordinal) {
        this.ordinal = ordinal;
    }

    /**
     * Returns the ordinal value (useful for doing a switch case based on the
     * type of CallStatus).
     *
     * @return the ordinal value.
     */
    public int ordinal() {
        return ordinal;
    }
}


