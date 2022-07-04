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

package com.hazelcast.spi.impl.eventservice.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.impl.eventservice.EventFilter;
import com.hazelcast.spi.impl.eventservice.EventRegistration;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.UUID;

public class Registration implements EventRegistration {

    private UUID id;
    private String serviceName;
    private String topic;
    private EventFilter filter;
    private Address subscriber;
    private transient boolean localOnly;
    private transient Object listener;

    public Registration() {
    }

    public Registration(@Nonnull UUID id, String serviceName, String topic,
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
    public UUID getId() {
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
        UUIDSerializationUtil.writeUUID(out, id);
        out.writeString(serviceName);
        out.writeString(topic);
        out.writeObject(subscriber);
        out.writeObject(filter);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        id = UUIDSerializationUtil.readUUID(in);
        serviceName = in.readString();
        topic = in.readString();
        subscriber = in.readObject();
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

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SpiDataSerializerHook.REGISTRATION;
    }

}
