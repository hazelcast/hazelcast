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

import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.impl.eventservice.impl.EventEnvelope;
import com.hazelcast.spi.impl.eventservice.impl.EventProcessor;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;

import java.io.IOException;

/**
 * An operation for sending a event to a remote subscriber (not on this node).
 * It will process the event on a thread defined by the {@link #orderKey} and in case of an exception,
 * the exception is returned to the caller.
 *
 * @see EventServiceImpl#sendEvent(Address, EventEnvelope, int)
 */
public class SendEventOperation extends Operation implements AllowedDuringPassiveState, IdentifiedDataSerializable {
    private EventEnvelope eventEnvelope;
    private int orderKey;

    public SendEventOperation() {
    }

    public SendEventOperation(EventEnvelope eventEnvelope, int orderKey) {
        this.eventEnvelope = eventEnvelope;
        this.orderKey = orderKey;
    }

    @Override
    public void run() throws Exception {
        EventServiceImpl eventService = (EventServiceImpl) getNodeEngine().getEventService();
        eventService.executeEventCallback(new EventProcessor(eventService, eventEnvelope, orderKey));
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        eventEnvelope.writeData(out);
        out.writeInt(orderKey);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        eventEnvelope = new EventEnvelope();
        eventEnvelope.readData(in);
        orderKey = in.readInt();
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SpiDataSerializerHook.SEND_EVENT;
    }
}
