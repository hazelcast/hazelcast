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

package com.hazelcast.topic;

import com.hazelcast.core.MessageListener;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.spi.annotation.Beta;

/**
 * A policy to handle error when dealing with the reliable topics. With this policy you can deal with:
 * <ol>
 *     <li>exception thrown while a message is processed using a {@link com.hazelcast.core.MessageListener}</li>
 *     <li>exception thrown while deserializing a message</li>
 *     <li>when a message listener is trying to read data from the ringbuffer, but the data doesn't exist anymore.
 *     For more information see the {@link StaleSequenceException}.
 *     </li>
 * </ol>
 *
 * @param <M>
 */
@Beta
public interface TopicErrorHandler<M extends MessageListener> {

    /**
     * Checks if the throwable should lead to a termination of the MessageListener.
     *
     * @param t the throwable
     * @param listener the that ran into problems.
     * @return true if the MessageListener should terminate itself, false otherwise.
     */
    boolean terminate(Throwable t, M listener);
}
