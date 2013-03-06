/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.impl.AbstractNamedOperation;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.locks.Lock;

/**
 * User: sancar
 * Date: 12/31/12
 * Time: 12:10 PM
 */
public class PublishOperation extends AbstractNamedOperation {

    private Data message;

    public PublishOperation() {
        super();
    }

    public PublishOperation(String name, Data message) {
        super(name);
        this.message = message;
    }

    @Override
    public void beforeRun() throws Exception {
        ((TopicService) getService()).incrementPublishes(name);
    }

    @Override
    public void run() throws Exception {
        TopicService service = getService();
        final Member publishingMember = getNodeEngine().getClusterService().getMember(getCallerAddress());
        TopicEvent topicEvent = new TopicEvent(name, message, publishingMember);
        final EventService eventService = getNodeEngine().getEventService();
        final Collection<EventRegistration> registrations = eventService.getRegistrations(TopicService.SERVICE_NAME, name);
        final Lock lock = service.getOrderLock(name);
        lock.lock();
        try {
            eventService.publishEvent(TopicService.SERVICE_NAME, registrations, topicEvent);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        message.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        message = new Data();
        message.readData(in);
    }
}
