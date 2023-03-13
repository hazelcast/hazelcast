/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.util.executor.StripedRunnable;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceSegment;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.internal.cluster.Versions.V5_3;

public class DeregistrationOperation extends AbstractRegistrationOperation implements Versioned {

    private String topic;
    private UUID id;
    private int orderKey = -1;

    public DeregistrationOperation() {
    }

    public DeregistrationOperation(String topic, UUID id, int orderKey, int memberListVersion) {
        super(memberListVersion);
        this.topic = topic;
        this.id = id;
        this.orderKey = orderKey;
    }

    @Override
    protected void runInternal() throws Exception {
        EventServiceImpl eventService = (EventServiceImpl) getNodeEngine().getEventService();
        eventService.executeEventCallback(new StripedRunnable() {
            @Override
            public void run() {
                EventServiceSegment segment = eventService.getSegment(getServiceName(), false);
                if (segment != null) {
                    if (id == null) {
                        segment.removeRegistrations(topic);
                    } else {
                        segment.removeRegistration(topic, id);
                    }
                }
            }

            @Override
            public int getKey() {
                return orderKey;
            }
        });
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    protected void writeInternalImpl(ObjectDataOutput out) throws IOException {
        out.writeString(topic);
        UUIDSerializationUtil.writeUUID(out, id);
        if (out.getVersion().isGreaterOrEqual(V5_3)) {
            out.writeInt(orderKey);
        }
    }

    @Override
    protected void readInternalImpl(ObjectDataInput in) throws IOException {
        topic = in.readString();
        id = UUIDSerializationUtil.readUUID(in);
        if (in.getVersion().isGreaterOrEqual(V5_3)) {
            orderKey = in.readInt();
        }
    }

    @Override
    public int getClassId() {
        return SpiDataSerializerHook.DEREGISTRATION;
    }
}
