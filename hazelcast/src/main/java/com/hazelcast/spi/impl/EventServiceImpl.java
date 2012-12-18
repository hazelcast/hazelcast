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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.executor.ExecutorThreadFactory;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.*;
import com.hazelcast.spi.*;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.util.ConcurrentHashSet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;

/**
 * @mdogan 12/14/12
 */

public class EventServiceImpl implements EventService {
    public static final EventRegistration[] EMPTY_REGISTRATIONS = new EventRegistration[0];

    private final ILogger logger;
    private final NodeEngineImpl nodeEngine;
    private final ConcurrentMap<String, EventServiceSegment> segments;
    final ExecutorService eventExecutorService;

    EventServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        logger = nodeEngine.getLogger(EventService.class.getName());
        Node node = nodeEngine.getNode();
        eventExecutorService = Executors.newSingleThreadExecutor(
                new ExecutorThreadFactory(node.threadGroup, node.hazelcastInstance,
                        node.getThreadNamePrefix("event"), node.getConfig().getClassLoader()));
        segments = new ConcurrentHashMap<String, EventServiceSegment>();
    }

    public EventRegistration registerListener(String serviceName, String topic, Object listener) {
        return registerListener(serviceName, topic, new EmptyFilter(), listener);
    }

    public EventRegistration registerListener(String serviceName, String topic, EventFilter filter, Object listener) {
        if (listener == null) {
            throw new IllegalArgumentException("Listener required!");
        }
        if (filter == null) {
            throw new IllegalArgumentException("Filter required");
        }
        EventServiceSegment segment = getSegment(serviceName, true);
        Registration reg = new Registration(createId(serviceName), serviceName, filter,
                nodeEngine.getThisAddress(), listener);

        if (segment.addRegistration(topic, reg)) {
            Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
            Collection<Future> calls = new ArrayList<Future>(members.size());
            for (MemberImpl member : members) {
                if (!member.localMember()) {
                    Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(serviceName,
                            new RegistrationOperation(topic, reg), member.getAddress()).build();
                    calls.add(inv.invoke());
                }
            }
            for (Future f : calls) {
                try {
                    f.get(5, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                } catch (TimeoutException ignored) {
                } catch (ExecutionException e) {
                    throw new HazelcastException(e);
                }
            }
            return reg;
        } else {
            return null;
        }
    }

    private String createId(String serviceName) {
        return serviceName + ":" + UUID.randomUUID().toString();
    }

    public boolean registerSubscriber(String serviceName, String topic, Registration reg) {
        EventServiceSegment segment = getSegment(serviceName, true);
        return segment.addRegistration(topic, reg);
    }

    public void deregisterListener(String serviceName, String id) {

    }

    public EventRegistration[] getRegistrationsAsArray(String serviceName, String topic) {
        final EventServiceSegment segment = getSegment(serviceName, false);
        if (segment != null) {
            final Collection<Registration> registrations = segment.getRegistrations(topic, false);
            return registrations != null && !registrations.isEmpty()
                    ? registrations.toArray(new Registration[registrations.size()])
                    : EMPTY_REGISTRATIONS;
        }
        return EMPTY_REGISTRATIONS;
    }

    public Collection<EventRegistration> getRegistrations(String serviceName, String topic) {
        final EventServiceSegment segment = getSegment(serviceName, false);
        if (segment != null) {
            final Collection<Registration> registrations = segment.getRegistrations(topic, false);
            return registrations != null && !registrations.isEmpty()
                    ? Collections.<EventRegistration>unmodifiableCollection(registrations)
                    : Collections.<EventRegistration>emptySet();
        }
        return Collections.emptySet();
    }

    public void publishEvent(String serviceName, EventRegistration registration, Object event) {
        if (!(registration instanceof Registration)) {
            throw new IllegalArgumentException();
        }
        final Registration reg = (Registration) registration;
        if (reg.isLocal()) {
            eventExecutorService.execute(new LocalEventDispatcher(serviceName, event, reg.listener));
        } else {
            final Address subscriber = registration.getSubscriber();
            final Data data = IOUtil.toData(new EventPacket(registration.getId(), serviceName, event));
            final Packet packet = new Packet(data);
            packet.setHeader(Packet.HEADER_EVENT, true);
            nodeEngine.getClusterService().send(packet, subscriber);
        }
    }

    public void publishEvent(String serviceName, Collection<EventRegistration> registrations, Object event) {
        final Iterator<EventRegistration> iter = registrations.iterator();
        final Data eventData = IOUtil.toData(event); // TODO: serialization might be optimized
        while (iter.hasNext()) {
            EventRegistration registration = iter.next();
            if (!(registration instanceof Registration)) {
                throw new IllegalArgumentException();
            }
            final Registration reg = (Registration) registration;
            if (reg.isLocal()) {
                eventExecutorService.execute(new LocalEventDispatcher(serviceName, event, reg.listener));
            } else {
                final Address subscriber = registration.getSubscriber();
                final Data data = IOUtil.toData(new EventPacket(registration.getId(), serviceName, eventData));
                final Packet packet = new Packet(data);
                packet.setHeader(Packet.HEADER_EVENT, true);
                nodeEngine.getClusterService().send(packet, subscriber);
            }
        }
    }

    public void executeEvent(Runnable eventRunnable) {
        eventExecutorService.execute(eventRunnable);
    }

    private EventServiceSegment getSegment(String service, boolean forceCreate) {
        EventServiceSegment segment = segments.get(service);
        if (segment == null && forceCreate) {
            segment = new EventServiceSegment(service);
            EventServiceSegment current = segments.putIfAbsent(service, segment);
            segment = current == null ? segment : current;
        }
        return segment;
    }

    @PrivateApi
    public void handleEvent(Packet packet) {
        eventExecutorService.execute(new EventPacketProcessor(packet));
    }

    void shutdown() {
        eventExecutorService.shutdownNow();
        segments.clear();
    }

    private class EventServiceSegment {
        final String serviceName;
        final ConcurrentMap<String, Collection<Registration>> registrations
                = new ConcurrentHashMap<String, Collection<Registration>>();

        final ConcurrentMap<String, Registration> registrationIdMap = new ConcurrentHashMap<String, Registration>();

        EventServiceSegment(String serviceName) {
            this.serviceName = serviceName;
        }

        private Collection<Registration> getRegistrations(String topic, boolean forceCreate) {
            Collection<Registration> listenerList = registrations.get(topic);
            if (listenerList == null && forceCreate) {
                listenerList = new ConcurrentHashSet<Registration>();
                Collection<Registration> current = registrations.putIfAbsent(topic, listenerList);
                listenerList = current == null ? listenerList : current;
            }
            return listenerList;
        }

        private boolean addRegistration(String topic, Registration registration) {
            final Collection<Registration> registrations = getRegistrations(topic, true);
            if (registrations.add(registration)) {
                registrationIdMap.put(registration.id, registration);
                return true;
            }
            return false;
        }
    }

    private class EventPacketProcessor implements Runnable {
        private Packet packet;

        public EventPacketProcessor(Packet packet) {
            this.packet = packet;
        }

        public void run() {
            Data data = packet.getValue();
            EventPacket eventPacket = IOUtil.toObject(data);
            final String serviceName = eventPacket.serviceName;
            EventPublishingService service = nodeEngine.getService(serviceName);
            if (service == null) {
                logger.log(Level.WARNING, "There is no service named: " + serviceName);
                return;
            }
            EventServiceSegment segment = getSegment(serviceName, false);
            if (segment == null) {
                logger.log(Level.WARNING, "No service registration found for " + serviceName);
                return;
            }
            Registration registration = segment.registrationIdMap.get(eventPacket.id);
            if (registration == null) {
                logger.log(Level.WARNING, "No registration found for " + serviceName + " / " + eventPacket.id);
                return;
            }
            if (!registration.isLocal()) {
                logger.log(Level.WARNING, "Invalid target for  " + registration);
                return;
            }
            service.dispatchEvent(eventPacket.event, registration.listener);
        }
    }

    private class LocalEventDispatcher implements Runnable {
        final String serviceName;
        final Object event;
        final Object listener;

        private LocalEventDispatcher(String serviceName, Object event, Object listener) {
            this.serviceName = serviceName;
            this.event = event;
            this.listener = listener;
        }

        public void run() {
            EventPublishingService service = nodeEngine.getService(serviceName);
            service.dispatchEvent(event, listener);
        }
    }

    static class Registration implements EventRegistration {
        private String id;
        private String serviceName;
        private EventFilter filter;
        private Address subscriber;
        private transient Object listener;

        public Registration() {
        }

        public Registration(String id, String serviceName, EventFilter filter, Address subscriber) {
            this(id, serviceName, filter, subscriber, null);
        }

        public Registration(String id, String serviceName, EventFilter filter, Address subscriber, Object listener) {
            this.filter = filter;
            this.id = id;
            this.listener = listener;
            this.serviceName = serviceName;
            this.subscriber = subscriber;
        }

        public EventFilter getFilter() {
            return filter;
        }

        public String getId() {
            return id;
        }

        public Address getSubscriber() {
            return subscriber;
        }

        public boolean isLocal() {
            return listener != null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Registration that = (Registration) o;

            if (!serviceName.equals(that.serviceName)) return false;
            if (!subscriber.equals(that.subscriber)) return false;
            if (!filter.equals(that.filter)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = serviceName.hashCode();
            result = 31 * result + filter.hashCode();
            result = 31 * result + subscriber.hashCode();
            return result;
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeUTF(id);
            out.writeUTF(serviceName);
            subscriber.writeData(out); // may be transient, subscriber == caller
            IOUtil.writeObject(out, filter);
        }

        public void readData(DataInput in) throws IOException {
            id = in.readUTF();
            serviceName = in.readUTF();
            subscriber = new Address();
            subscriber.readData(in);
            filter = IOUtil.readObject(in);
        }


        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("Registration");
            sb.append("{filter=").append(filter);
            sb.append(", id='").append(id).append('\'');
            sb.append(", serviceName='").append(serviceName).append('\'');
            sb.append(", subscriber=").append(subscriber);
            sb.append(", listener=").append(listener);
            sb.append('}');
            return sb.toString();
        }
    }

    static class EventPacket implements DataSerializable {

        String id;
        String serviceName;
        Object event;

        EventPacket() {
        }

        EventPacket(String id, String serviceName, Object event) {
            this.event = event;
            this.id = id;
            this.serviceName = serviceName;
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeUTF(id);
            out.writeUTF(serviceName);
            IOUtil.writeObject(out, event);
        }

        public void readData(DataInput in) throws IOException {
            id = in.readUTF();
            serviceName = in.readUTF();
            event = IOUtil.readObject(in);
        }
    }

    static class EmptyFilter implements EventFilter, DataSerializable {
        public boolean eval(Object arg) {
            return true;
        }
        public void writeData(DataOutput out) throws IOException {}
        public void readData(DataInput in) throws IOException {}
    }

    static class RegistrationOperation extends AbstractOperation {

        private String topic;
        private Registration registration;
        private boolean response = false;

        RegistrationOperation() {
        }

        private RegistrationOperation(String topic, Registration registration) {
            this.registration = registration;
            this.topic = topic;
        }

        public void run() throws Exception {
            EventServiceImpl eventService = (EventServiceImpl) getNodeEngine().getEventService();
            response = eventService.registerSubscriber(getServiceName(), topic, registration);
        }

        @Override
        public Object getResponse() {
            return response;
        }

        @Override
        public boolean returnsResponse() {
            return true;
        }

        @Override
        protected void writeInternal(DataOutput out) throws IOException {
            out.writeUTF(topic);
            registration.writeData(out);
        }

        @Override
        protected void readInternal(DataInput in) throws IOException {
            topic = in.readUTF();
            registration = new Registration();
            registration.readData(in);
        }
    }
}
