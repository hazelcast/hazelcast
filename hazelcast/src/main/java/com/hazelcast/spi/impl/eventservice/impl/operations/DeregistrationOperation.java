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

package com.hazelcast.spi.impl.eventservice.impl.operations;

import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceSegment;

import java.io.IOException;
import java.util.UUID;

public class DeregistrationOperation extends AbstractRegistrationOperation {

    private String topic;
    private UUID id;

    public DeregistrationOperation() {
    }

    public DeregistrationOperation(String topic, UUID id, int memberListVersion) {
        super(memberListVersion);
        this.topic = topic;
        this.id = id;
    }

    @Override
    protected void runInternal() throws Exception {
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
    protected void writeInternalImpl(ObjectDataOutput out) throws IOException {
        out.writeString(topic);
        UUIDSerializationUtil.writeUUID(out, id);
    }

    @Override
    protected void readInternalImpl(ObjectDataInput in) throws IOException {
        topic = in.readString();
        id = UUIDSerializationUtil.readUUID(in);
    }

    @Override
    public int getClassId() {
        return SpiDataSerializerHook.DEREGISTRATION;
    }
}
