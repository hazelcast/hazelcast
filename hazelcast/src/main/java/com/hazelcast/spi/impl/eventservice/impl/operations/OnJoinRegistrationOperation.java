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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;
import com.hazelcast.spi.impl.eventservice.impl.Registration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class OnJoinRegistrationOperation extends Operation implements IdentifiedDataSerializable {

    private Collection<Registration> registrations;

    public OnJoinRegistrationOperation() {
    }

    public OnJoinRegistrationOperation(Collection<Registration> registrations) {
        this.registrations = registrations;
    }

    @Override
    public void run() throws Exception {
        if (registrations == null || registrations.size() <= 0) {
            return;
        }

        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        EventServiceImpl eventService = (EventServiceImpl) nodeEngine.getEventService();
        for (Registration reg : registrations) {
            eventService.handleRegistration(reg);
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        int len = registrations != null ? registrations.size() : 0;
        out.writeInt(len);
        if (len > 0) {
            for (Registration reg : registrations) {
                reg.writeData(out);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int len = in.readInt();
        if (len > 0) {
            registrations = new ArrayList<Registration>(len);
            for (int i = 0; i < len; i++) {
                Registration reg = new Registration();
                registrations.add(reg);
                reg.readData(in);
            }
        }
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SpiDataSerializerHook.ON_JOIN_REGISTRATION;
    }
}
