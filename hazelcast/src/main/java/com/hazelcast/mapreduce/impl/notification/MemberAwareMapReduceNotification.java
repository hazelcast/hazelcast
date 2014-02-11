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

package com.hazelcast.mapreduce.impl.notification;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Base class for all notifications based on a member
 */
public abstract class MemberAwareMapReduceNotification
        extends MapReduceNotification {

    private Address address;

    protected MemberAwareMapReduceNotification() {
    }

    protected MemberAwareMapReduceNotification(Address address, String name, String jobId) {
        super(name, jobId);
        this.address = address;
    }

    public Address getAddress() {
        return address;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        super.writeData(out);
        address.writeData(out);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        super.readData(in);
        address = new Address();
        address.readData(in);
    }

    @Override
    public String toString() {
        return "MemberAwareMapReduceNotification{" + "address=" + address + '}';
    }

}
