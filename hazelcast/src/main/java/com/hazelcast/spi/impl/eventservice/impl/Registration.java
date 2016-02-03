/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;

import java.io.IOException;

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
        this.filter = filter;
        this.id = id;
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

    //CHECKSTYLE:OFF
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Registration that = (Registration) o;

        if (id != null ? !id.equals(that.id) : that.id != null) {
            return false;
        }
        if (serviceName != null ? !serviceName.equals(that.serviceName) : that.serviceName != null) {
            return false;
        }
        if (topic != null ? !topic.equals(that.topic) : that.topic != null) {
            return false;
        }
        if (filter != null ? !filter.equals(that.filter) : that.filter != null) {
            return false;
        }
        if (subscriber != null ? !subscriber.equals(that.subscriber) : that.subscriber != null) {
            return false;
        }

        return true;
    }
    //CHECKSTYLE:ON

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (serviceName != null ? serviceName.hashCode() : 0);
        result = 31 * result + (topic != null ? topic.hashCode() : 0);
        result = 31 * result + (filter != null ? filter.hashCode() : 0);
        result = 31 * result + (subscriber != null ? subscriber.hashCode() : 0);
        return result;
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
