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

package com.hazelcast.cluster;

import com.hazelcast.nio.Address;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Bind extends Master {

    private Address targetAddress;
    private boolean replyBack = false;

    public Bind() {
    }

    public Bind(Address localAddress) {
        super(localAddress);
        this.targetAddress = null;
    }

    public Bind(Address localAddress, final Address targetAddress, final boolean replyBack) {
        super(localAddress);
        this.targetAddress = targetAddress;
        this.replyBack = replyBack;
    }

    public void process() {
        getNode().connectionManager.bind(getConnection(), address, targetAddress, replyBack);
    }

    @Override
    public void readData(final DataInput in) throws IOException {
        super.readData(in);
        boolean hasTarget = in.readBoolean();
        if (hasTarget) {
            targetAddress = new Address();
            targetAddress.readData(in);
        }
        replyBack = in.readBoolean();
    }

    @Override
    public void writeData(final DataOutput out) throws IOException {
        super.writeData(out);
        boolean hasTarget = targetAddress != null;
        out.writeBoolean(hasTarget);
        if (hasTarget) {
            targetAddress.writeData(out);
        }
        out.writeBoolean(replyBack);
    }

    @Override
    public String toString() {
        return "Bind " + address;
    }
}
