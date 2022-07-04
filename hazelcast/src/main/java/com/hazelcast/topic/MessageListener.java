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

package com.hazelcast.topic;

import java.util.EventListener;

/**
 * Message listener for {@link ITopic}.
 * <p>
 * A MessageListener will never be called concurrently (provided that it's not registered twice). So there
 * is no need to synchronize access to the state it reads or writes. Also there is no need to synchronize
 * when publishing to the ITopic, the ITopic is responsible for memory consistency effects. In other words,
 * there is no need to make fields of the MessageListener volatile or access them using synchronized blocks.
 *
 * @param <E> message type
 */
@FunctionalInterface
public interface MessageListener<E> extends EventListener {

    /**
     * Invoked when a message is received for the topic. Note that the topic guarantees message ordering.
     * Therefore there is only one thread invoking onMessage. The user should not keep the thread busy, but preferably
     * should dispatch it via an Executor. This will increase the performance of the topic.
     *
     * @param message the message that is received for the topic
     */
    void onMessage(Message<E> message);
}
