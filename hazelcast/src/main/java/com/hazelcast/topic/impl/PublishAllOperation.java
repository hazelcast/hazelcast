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

package com.hazelcast.topic.impl;

import com.hazelcast.config.TopicConfig;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.operationservice.AbstractNamedOperation;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.locks.Lock;

/**
 * ITopic publication operation used when global ordering is enabled
 * (all nodes listening to the same topic get their messages in the same order).
 *
 * @see TotalOrderedTopicProxy
 * @see TopicConfig#isGlobalOrderingEnabled()
 */
public class PublishAllOperation extends AbstractNamedOperation
        implements IdentifiedDataSerializable {

    private Data[] messages;

    public PublishAllOperation() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public PublishAllOperation(String name, Data[] messages) {
        super(name);
        this.messages = messages;
    }

    @Override
    public void run() throws Exception {
        TopicService service = getService();
        EventService eventService = getNodeEngine().getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(TopicService.SERVICE_NAME, name);

        Lock lock = service.getOrderLock(name);
        lock.lock();
        try {
            for (Data item : messages) {
                TopicEvent topicEvent = new TopicEvent(name, item, getCallerAddress());
                eventService.publishEvent(TopicService.SERVICE_NAME, registrations, topicEvent, name.hashCode());
                service.incrementPublishes(name);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int getFactoryId() {
        return TopicDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return TopicDataSerializerHook.PUBLISH_ALL;
    }

    @Override
    public String getServiceName() {
        return TopicService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(messages.length);
        for (Data item : messages) {
            IOUtil.writeData(out, item);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int length = in.readInt();
        messages = new Data[length];
        for (int k = 0; k < messages.length; k++) {
            messages[k] = IOUtil.readData(in);
        }
    }
}
