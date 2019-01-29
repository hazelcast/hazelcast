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

package com.hazelcast.spi;

/**
 * The result of an {@link Operation#call()}.
 *
 * Using the CallStatus the operation can control how the system will deal with the operation after it is executed. For example
 * when the CallStatus is {@link CallStatus#DONE_RESPONSE}, a response can be send to the caller. But it also allows for
 * different behavior where no response is available yet e.g. when an operation gets offloaded.
 *
 * <h1>CallStatus doesn't need ot be enum</h1>
 * CallStatus is currently an enumeration. But if for whatever reason state needs to be returned, we can easily convert this
 * enumeration to a regular object like e.g. java https://docs.oracle.com/javase/8/docs/api/java/util/Optional.html.
 *
 * So you can have a call status class and if e.g. an operation that requires interleaving, a new Interleaving (subclass of
 * CallStatus) can be returned containing all the state for that interleaving.
 *
 * The same can be done for Blocking operations. Currently we just return WAIT, but this can easily be modified by creating a new
 * Wait (subclass of CallStatus) containing the wait key or whatever else is needed.
 *
 * It is very likely that the CallStatus is going to be converted to regular classes/object with the introduction of the
 * 'Offloaded' which is part of phase 2 for the Operation continuations.
 *
 * <h1>Future additions</h1>
 * In the future we can add more values to this enumeration, for example 'YIELD' for batching operations that wants to
 * release the operation thread so that other operations can be interleaved.
 */
public enum CallStatus {

    /**
     * Signals that the Operation is done running and that a response is ready to be returned. Most of the normal operations
     * like IAtomicLong.get will fall in this category.
     */
    DONE_RESPONSE,

    /**
     * Signals that the Operation is done running, but no response will be returned. Most of the regular operations like map.get
     * will return a response, but there are also fire and forget operations (lot of cluster operations) that don't return a
     * response.
     */
    DONE_VOID,

    /**
     * Indicates that the call could not complete because waiting is required. E.g. a queue.take on an empty queue. This can
     * only be returned by BlockingOperations.
     */
    WAIT,

    /**
     * Signals that the Operation has been offloaded e.g. an EntryProcessor. And therefor no response is available when the
     * operation is executed. Only at a later time a response is ready and it is up to the offload functionality to determine
     * how to deal with that. It could be that a response is send using the original operation handler, but it could also
     * be that the operation will be rescheduled on an operation thread (a real continuation).
     */
    OFFLOADED
}
