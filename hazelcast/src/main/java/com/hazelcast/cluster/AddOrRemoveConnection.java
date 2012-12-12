/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

public class AddOrRemoveConnection extends AbstractRemotelyProcessable {
    public Address address = null;

    public boolean add = true;

    public AddOrRemoveConnection() {
    }

    public AddOrRemoveConnection(Address address, boolean add) {
        super();
        this.address = address;
        this.add = add;
    }

    @Override
    public void readData(DataInput in) throws IOException {
        address = new Address();
        address.readData(in);
        add = in.readBoolean();
    }

    @Override
    public void writeData(DataOutput out) throws IOException {
        address.writeData(out);
        out.writeBoolean(add);
    }

    @Override
    public String toString() {
        return "AddOrRemoveConnection add=" + add + ", " + address;
    }

    public void process() {
        node.clusterManager.handleAddRemoveConnection(this);
    }
}
