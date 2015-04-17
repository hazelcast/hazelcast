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

package com.hazelcast.spi.impl.eventservice.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceSegment;

import java.io.IOException;

public class DeregistrationOperation extends AbstractOperation {

    private String topic;
    private String id;

    public DeregistrationOperation() {
    }

    public DeregistrationOperation(String topic, String id) {
        this.topic = topic;
        this.id = id;
    }

    @Override
    public void run() throws Exception {
        EventServiceImpl eventService = (EventServiceImpl) getNodeEngine().getEventService();
        EventServiceSegment segment = eventService.getSegment(getServiceName(), false);
        if (segment != null) {
            segment.removeRegistration(topic, id);
        }
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(topic);
        out.writeUTF(id);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        topic = in.readUTF();
        id = in.readUTF();
    }
}
