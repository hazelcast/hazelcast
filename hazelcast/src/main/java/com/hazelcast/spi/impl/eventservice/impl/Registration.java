/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.eventservice.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.BinaryInterface;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.util.Preconditions;

import java.io.IOException;

// Even though this class is not used in client-member communication, we annotate it to be excluded from related tests, as it
// cannot implement IdentifiedDataSerializable (both EventRegistration and IdentifiedDataSerializable interfaces define a
// method {@code getId()} which differ in return type).
// Since this class is still DataSerializable it should not be moved/renamed in 3.9, otherwise upgradability will be compromised.
@BinaryInterface
public class Registration implements EventRegistration {

    private String id;
    private String serviceName;
    private String topic;
    private EventFilter filter;
    private Address subscriber;
    private transient boolean localOnly;
    private transient Object listener;

    public Registration() {
    }

    public Registration(String id, String serviceName, String topic,
                        EventFilter filter, Address subscriber, Object listener, boolean localOnly) {
        this.id = Preconditions.checkNotNull(id, "Registration ID cannot be null!");
        this.filter = filter;
        this.listener = listener;
        this.serviceName = serviceName;
        this.topic = topic;
        this.subscriber = subscriber;
        this.localOnly = localOnly;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public EventFilter getFilter() {
        return filter;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public Address getSubscriber() {
        return subscriber;
    }

    @Override
    public boolean isLocalOnly() {
        return localOnly;
    }

    public Object getListener() {
        return listener;
    }

    // Registration equals() and hashCode() relies on the ID field only,
    // because the registration ID is unique in the cluster
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Registration)) {
            return false;
        }
        Registration that = (Registration) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(serviceName);
        out.writeUTF(topic);
        subscriber.writeData(out);
        out.writeObject(filter);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        id = in.readUTF();
        serviceName = in.readUTF();
        topic = in.readUTF();
        subscriber = new Address();
        subscriber.readData(in);
        filter = in.readObject();
    }

    @Override
    public String toString() {
        return "Registration{"
                + "filter=" + filter
                + ", id='" + id + '\''
                + ", serviceName='" + serviceName + '\''
                + ", subscriber=" + subscriber
                + ", listener=" + listener
                + '}';
    }
}
