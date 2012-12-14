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

package com.hazelcast.spi.impl;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.EventOperation;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.util.ConcurrentHashSet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * @mdogan 12/12/12
 */

@PrivateApi
class EventServiceImpl implements EventService {

    private final NodeEngineImpl nodeService;
    private final ConcurrentMap<String, EventSegment> segments;

    EventServiceImpl(NodeEngineImpl nodeService) {
        this.nodeService = nodeService;
        segments = new ConcurrentHashMap<String, EventSegment>();
    }

    void registerSubscribers(String service, Object topic, Address... subscribers) {
        Collection<MemberImpl> members = nodeService.getClusterService().getMemberList();
        Collection<Future> calls = new ArrayList<Future>(members.size());
        for (MemberImpl member : members) {
            if (!member.localMember()) {
                Invocation inv = nodeService.getInvocationService().createInvocationBuilder(service,
                        new RegistrationOperation(service, subscribers, topic), member.getAddress()).build();
                calls.add(inv.invoke());
            }
        }
        final EventSegment segment = getSegment(service, true);
        Collection<Address> subscriberList = getSubscribers(topic, segment, true);
        Collections.addAll(subscriberList, subscribers);
        for (Future call : calls) {
            try {
                call.get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                nodeService.getLogger(EventServiceImpl.class.getName()).log(Level.FINEST, "While registering listener", e);
            }
        }
    }

    void publishEvent(EventOperation event) {
        final EventSegment segment = getSegment(event.getServiceName(), false);
        if (segment == null) return;
        final Collection<Address> subscriberList = getSubscribers(event.getTopic(), segment, false);
        if (subscriberList == null) return;

        final Data eventData = IOUtil.toData(event);
        final ClusterService clusterService = nodeService.getClusterService();
        for (Address target : subscriberList) {
            Packet p = new Packet(eventData);
            p.setHeader(Packet.HEADER_EVENT, true);
            clusterService.send(p, target); // TODO: need flow control
        }
    }

    void onEvent(EventOperation event) {
        final EventSegment segment = getSegment(event.getServiceName(), false);
        if (segment == null) return;
        final Collection<Object> subscriberList = getListeners(event.getTopic(), segment, false);
        if (subscriberList == null) return;


    }

    private Collection<Address> getSubscribers(Object topic, EventSegment segment, boolean forceCreate) {
        Collection<Address> subscriberList = segment.subscriberMap.get(topic);
        if (subscriberList == null && forceCreate) {
            subscriberList = new ConcurrentHashSet<Address>();
            Collection<Address> current = segment.subscriberMap.putIfAbsent(topic, subscriberList);
            subscriberList = current == null ? subscriberList : current;
        }
        return subscriberList;
    }

    private Collection<Object> getListeners(Object topic, EventSegment segment, boolean forceCreate) {
        Collection<Object> listenerList = segment.listenerMap.get(topic);
        if (listenerList == null && forceCreate) {
            listenerList = new ConcurrentHashSet<Object>();
            Collection<Object> current = segment.listenerMap.putIfAbsent(topic, listenerList);
            listenerList = current == null ? listenerList : current;
        }
        return listenerList;
    }

    private EventSegment getSegment(String service, boolean forceCreate) {
        EventSegment segment = segments.get(service);
        if (segment == null && forceCreate) {
            segment = new EventSegment(service);
            EventSegment current = segments.putIfAbsent(service, segment);
            segment = current == null ? segment : current;
        }
        return segment;
    }


    private class EventSegment {
        final String serviceName;
        final ConcurrentMap<Object, Collection<Address>> subscriberMap = new ConcurrentHashMap<Object, Collection<Address>>();
        final ConcurrentMap<Object, Collection<Object>> listenerMap = new ConcurrentHashMap<Object, Collection<Object>>();

        EventSegment(String serviceName) {
            this.serviceName = serviceName;
        }
    }

    static class RegistrationOperation extends AbstractOperation {

        private String eventServiceName;
        private Object topic;
        private Address[] addresses;

        RegistrationOperation() {
        }

        private RegistrationOperation(String eventServiceName, Address[] addresses, Object topic) {
            this.eventServiceName = eventServiceName;
            this.addresses = addresses;
            this.topic = topic;
        }

        public void run() throws Exception {
            EventServiceImpl eventSupport = (EventServiceImpl) getNodeEngine().getEventService();
            eventSupport.registerSubscribers(eventServiceName, topic, addresses);
        }

        @Override
        public Object getResponse() {
            return Boolean.TRUE;
        }

        @Override
        public boolean returnsResponse() {
            return true;
        }

        @Override
        protected void writeInternal(DataOutput out) throws IOException {
            out.writeUTF(eventServiceName);
            IOUtil.writeObject(out, topic);
            int len = addresses != null ? addresses.length : 0;
            out.writeInt(len);
            if (len > 0) {
                for (Address address : addresses) {
                    address.writeData(out);
                }
            }
        }

        @Override
        protected void readInternal(DataInput in) throws IOException {
            eventServiceName = in.readUTF();
            topic = IOUtil.readObject(in);
            int len = in.readInt();
            addresses = new Address[len];
            for (int i = 0; i < len; i++) {
                addresses[i] = new Address();
                addresses[i].readData(in);
            }
        }
    }
}
