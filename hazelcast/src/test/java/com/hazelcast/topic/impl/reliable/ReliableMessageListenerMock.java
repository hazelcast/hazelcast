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

package com.hazelcast.topic.impl.reliable;

import com.hazelcast.topic.Message;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.topic.ReliableMessageListener;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class ReliableMessageListenerMock implements ReliableMessageListener<String> {

    private final ILogger logger = Logger.getLogger(ReliableMessageListenerMock.class);
    public final List<String> objects = new CopyOnWriteArrayList<String>();
    public final List<Message<String>> messages = new CopyOnWriteArrayList<Message<String>>();
    public volatile long storedSequence;
    public volatile boolean isLossTolerant = false;
    public volatile long initialSequence = -1;
    public volatile boolean isTerminal = true;

    @Override
    public void onMessage(Message<String> message) {
        objects.add(message.getMessageObject());
        messages.add(message);
        logger.info("Received: " + message.getMessageObject());
    }

    @Override
    public long retrieveInitialSequence() {
        return initialSequence;
    }

    @Override
    public void storeSequence(long sequence) {
        storedSequence = sequence;
    }

    @Override
    public boolean isLossTolerant() {
        return isLossTolerant;
    }

    @Override
    public boolean isTerminal(Throwable failure) {
        return isTerminal;
    }

    public void clean() {
        objects.clear();
        messages.clear();
    }
}
