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
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;
import com.hazelcast.spi.impl.eventservice.impl.Registration;

import java.io.IOException;

public class RegistrationOperation extends AbstractRegistrationOperation {

    private Registration registration;
    private boolean response;

    public RegistrationOperation() {
    }

    public RegistrationOperation(Registration registration, int memberListVersion) {
        super(memberListVersion);
        this.registration = registration;
    }

    @Override
    protected void runInternal() {
        EventServiceImpl eventService = (EventServiceImpl) getNodeEngine().getEventService();
        response = eventService.handleRegistration(registration);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void writeInternalImpl(ObjectDataOutput out) throws IOException {
        registration.writeData(out);
    }

    @Override
    protected void readInternalImpl(ObjectDataInput in) throws IOException {
        registration = new Registration();
        registration.readData(in);
    }

    @Override
    public int getClassId() {
        return SpiDataSerializerHook.REGISTRATION_OPERATION;
    }
}
