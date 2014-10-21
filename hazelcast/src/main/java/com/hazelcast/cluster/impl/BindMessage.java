/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class BindMessage implements IdentifiedDataSerializable {

    private Address localAddress;
    private Address targetAddress;
    private boolean reply;

    public BindMessage() {
    }

    public BindMessage(Address localAddress, Address targetAddress, boolean reply) {
        this.localAddress = localAddress;
        this.targetAddress = targetAddress;
        this.reply = reply;
    }

    public Address getLocalAddress() {
        return localAddress;
    }

    public Address getTargetAddress() {
        return targetAddress;
    }

    public boolean shouldReply() {
        return reply;
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.BIND_MESSAGE;
    }

    @Override
    public void readData(final ObjectDataInput in) throws IOException {
        localAddress = new Address();
        localAddress.readData(in);
        boolean hasTarget = in.readBoolean();
        if (hasTarget) {
            targetAddress = new Address();
            targetAddress.readData(in);
        }
        reply = in.readBoolean();
    }

    @Override
    public void writeData(final ObjectDataOutput out) throws IOException {
        localAddress.writeData(out);
        boolean hasTarget = targetAddress != null;
        out.writeBoolean(hasTarget);
        if (hasTarget) {
            targetAddress.writeData(out);
        }
        out.writeBoolean(reply);
    }

    @Override
    public String toString() {
        return "Bind " + localAddress;
    }
}
