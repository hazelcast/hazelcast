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

import com.hazelcast.core.Member;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.InternalEventService;
import com.hazelcast.spi.impl.eventservice.impl.operations.DeregistrationOperation;
import com.hazelcast.spi.impl.eventservice.impl.operations.PostJoinRegistrationOperation;
import com.hazelcast.spi.impl.eventservice.impl.operations.RegistrationOperation;
import com.hazelcast.spi.impl.eventservice.impl.operations.SendEventOperation;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.executor.StripedExecutor;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.util.counters.MwCounter.newMwCounter;
import static com.hazelcast.util.FutureUtil.ExceptionHandler;
import static com.hazelcast.util.FutureUtil.waitWithDeadline;

public class EventServiceImpl implements InternalEventService, MetricsProvider {

    public static final String EVENT_SYNC_FREQUENCY_PROP = "hazelcast.event.sync.frequency";

    private static final EventRegistration[] EMPTY_REGISTRATIONS = new EventRegistration[0];

    private static final int EVENT_SYNC_FREQUENCY = 100000;
    private static final int SEND_RETRY_COUNT = 50;
    private static final int SEND_EVENT_TIMEOUT_SECONDS = 5;
    private static final int REGISTRATION_TIMEOUT_SECONDS = 5;
    private static final int DEREGISTER_TIMEOUT_SECONDS = 5;
    private static final int WARNING_LOG_FREQUENCY = 1000;

    final ILogger logger;
    final NodeEngineImpl nodeEngine;

    private final ExceptionHandler registrationExceptionHandler;
    private final ExceptionHandler deregistrationExceptionHandler;
    private final ConcurrentMap<String, EventServiceSegment> segments;
    private final StripedExecutor eventExecutor;
    private final long eventQueueTimeoutMs;

    @Probe(name = "threadCount")
    private final int eventThreadCount;
    @Probe(name = "queueCapacity")
    private final int eventQueueCapacity;
    @Probe(name = "totalFailureCount")
    private final MwCounter totalFailures = newMwCounter();
    @Probe(name = "rejectedCount")
    private final MwCounter rejectedCount = newMwCounter();
    @Probe(name = "syncDeliveryFailureCount")
    private final MwCounter syncDeliveryFailureCount = newMwCounter();

    private final InternalSerializationService serializationService;
    private final int eventSyncFrequency;

    public EventServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
        this.logger = nodeEngine.getLogger(EventService.class.getName());
        HazelcastProperties hazelcastProperties = nodeEngine.getProperties();
        this.eventThreadCount = hazelcastProperties.getInteger(GroupProperty.EVENT_THREAD_COUNT);
        this.eventQueueCapacity = hazelcastProperties.getInteger(GroupProperty.EVENT_QUEUE_CAPACITY);
        this.eventQueueTimeoutMs = hazelcastProperties.getMillis(GroupProperty.EVENT_QUEUE_TIMEOUT_MILLIS);
        int eventSyncFrequency;
        try {
            eventSyncFrequency = Integer.parseInt(System.getProperty(EVENT_SYNC_FREQUENCY_PROP));
            if (eventSyncFrequency <= 0) {
                eventSyncFrequency = EVENT_SYNC_FREQUENCY;
            }
        } catch (Exception e) {
            eventSyncFrequency = EVENT_SYNC_FREQUENCY;
        }
        this.eventSyncFrequency = eventSyncFrequency;

        HazelcastThreadGroup threadGroup = nodeEngine.getNode().getHazelcastThreadGroup();
        this.eventExecutor = new StripedExecutor(
                nodeEngine.getNode().getLogger(EventServiceImpl.class),
                threadGroup.getThreadNamePrefix("event"),
                threadGroup.getInternalThreadGroup(),
                eventThreadCount,
                eventQueueCapacity);
        this.registrationExceptionHandler
                = new FutureUtilExceptionHandler(logger, "Member left while registering listener...");
        this.deregistrationExceptionHandler
                = new FutureUtilExceptionHandler(logger, "Member left while de-registering listener...");
        this.segments = new ConcurrentHashMap<String, EventServiceSegment>();
    }

    @Override
    public void provideMetrics(MetricsRegistry registry) {
        registry.scanAndRegister(this, "event");
    }

    @Override
    public void close(EventRegistration eventRegistration) {
        Registration registration = (Registration) eventRegistration;

        Object listener = registration.getListener();
        if (!(listener instanceof Closeable)) {
            return;
        }

        try {
            ((Closeable) listener).close();
        } catch (IOException e) {
            EmptyStatement.ignore(e);
        }
    }

    @Override
    public int getEventThreadCount() {
        return eventThreadCount;
    }

    @Override
    public int getEventQueueCapacity() {
        return eventQueueCapacity;
    }

    @Probe(name = "eventQueueSize", level = MANDATORY)
    @Override
    public int getEventQueueSize() {
        return eventExecutor.getWorkQueueSize();
    }

    @Probe(level = MANDATORY)
    private long eventsProcessed() {
        return eventExecutor.processedCount();
    }

    @Override
    public EventRegistration registerLocalListener(String serviceName, String topic, Object listener) {
        return registerListenerInternal(serviceName, topic, TrueEventFilter.INSTANCE, listener, true);
    }

    @Override
    public EventRegistration registerLocalListener(String serviceName, String topic, EventFilter filter, Object listener) {
        return registerListenerInternal(serviceName, topic, filter, listener, true);
    }

    @Override
    public EventRegistration registerListener(String serviceName, String topic, Object listener) {
        return registerListenerInternal(serviceName, topic, TrueEventFilter.INSTANCE, listener, false);
    }

    @Override
    public EventRegistration registerListener(String serviceName, String topic, EventFilter filter, Object listener) {
        return registerListenerInternal(serviceName, topic, filter, listener, false);
    }

    private EventRegistration registerListenerInternal(String serviceName, String topic, EventFilter filter, Object listener,
                                                       boolean localOnly) {
        if (listener == null) {
            throw new IllegalArgumentException("Listener required!");
        }
        if (filter == null) {
            throw new IllegalArgumentException("EventFilter required!");
        }
        EventServiceSegment segment = getSegment(serviceName, true);
        String id = UuidUtil.newUnsecureUuidString();
        Registration reg = new Registration(id, serviceName, topic, filter, nodeEngine.getThisAddress(), listener, localOnly);
        if (!segment.addRegistration(topic, reg)) {
            return null;
        }

        if (!localOnly) {
            invokeRegistrationOnOtherNodes(serviceName, reg);
        }
        return reg;
    }

    public boolean handleRegistration(Registration reg) {
        if (nodeEngine.getThisAddress().equals(reg.getSubscriber())) {
            return false;
        }
        EventServiceSegment segment = getSegment(reg.getServiceName(), true);
        return segment.addRegistration(reg.getTopic(), reg);
    }

    @Override
    public boolean deregisterListener(String serviceName, String topic, Object id) {
        final EventServiceSegment segment = getSegment(serviceName, false);
        if (segment != null) {
            final Registration reg = segment.removeRegistration(topic, String.valueOf(id));
            if (reg != null && !reg.isLocalOnly()) {
                invokeDeregistrationOnOtherNodes(serviceName, topic, String.valueOf(id));
            }
            return reg != null;
        }
        return false;
    }

    @Override
    public void deregisterAllListeners(String serviceName, String topic) {
        final EventServiceSegment segment = getSegment(serviceName, false);
        if (segment != null) {
            segment.removeRegistrations(topic);
        }
    }

    private void invokeRegistrationOnOtherNodes(String serviceName, Registration reg) {
        OperationService operationService = nodeEngine.getOperationService();
        Collection<Member> members = nodeEngine.getClusterService().getMembers();
        Collection<Future> calls = new ArrayList<Future>(members.size());
        for (Member member : members) {
            if (!member.localMember()) {
                RegistrationOperation operation = new RegistrationOperation(reg);
                Future f = operationService.invokeOnTarget(serviceName, operation, member.getAddress());
                calls.add(f);
            }
        }

        waitWithDeadline(calls, REGISTRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS, registrationExceptionHandler);
    }

    private void invokeDeregistrationOnOtherNodes(String serviceName, String topic, String id) {
        OperationService operationService = nodeEngine.getOperationService();
        Collection<Member> members = nodeEngine.getClusterService().getMembers();
        Collection<Future> calls = new ArrayList<Future>(members.size());
        for (Member member : members) {
            if (!member.localMember()) {
                DeregistrationOperation operation = new DeregistrationOperation(topic, id);
                Future f = operationService.invokeOnTarget(serviceName, operation, member.getAddress());
                calls.add(f);
            }
        }

        waitWithDeadline(calls, DEREGISTER_TIMEOUT_SECONDS, TimeUnit.SECONDS, deregistrationExceptionHandler);
    }

    @Override
    public EventRegistration[] getRegistrationsAsArray(String serviceName, String topic) {
        final EventServiceSegment segment = getSegment(serviceName, false);
        if (segment != null) {
            final Collection<Registration> registrations = segment.getRegistrations(topic, false);
            if (registrations == null || registrations.isEmpty()) {
                return EMPTY_REGISTRATIONS;
            } else {
                return registrations.toArray(new Registration[registrations.size()]);
            }
        }
        return EMPTY_REGISTRATIONS;
    }

    @Override
    public Collection<EventRegistration> getRegistrations(String serviceName, String topic) {
        final EventServiceSegment segment = getSegment(serviceName, false);
        if (segment != null) {
            final Collection<Registration> registrations = segment.getRegistrations(topic, false);
            if (registrations == null || registrations.isEmpty()) {
                return Collections.<EventRegistration>emptySet();
            } else {
                return Collections.<EventRegistration>unmodifiableCollection(registrations);
            }
        }
        return Collections.emptySet();
    }

    @Override
    public boolean hasEventRegistration(String serviceName, String topic) {
        final EventServiceSegment segment = getSegment(serviceName, false);
        if (segment != null) {
            return segment.hasRegistration(topic);
        }
        return false;
    }

    @Override
    public void publishEvent(String serviceName, String topic, Object event, int orderKey) {
        Collection<EventRegistration> registrations = getRegistrations(serviceName, topic);
        publishEvent(serviceName, registrations, event, orderKey);
    }

    @Override
    public void publishEvent(String serviceName, EventRegistration registration, Object event, int orderKey) {
        if (!(registration instanceof Registration)) {
            throw new IllegalArgumentException();
        }

        if (isLocal(registration)) {
            executeLocal(serviceName, event, registration, orderKey);
        } else {
            EventEnvelope eventEnvelope = new EventEnvelope(registration.getId(), serviceName, event);
            sendEvent(registration.getSubscriber(), eventEnvelope, orderKey);
        }
    }

    @Override
    public void publishEvent(String serviceName, Collection<EventRegistration> registrations, Object event, int orderKey) {
        Data eventData = null;
        for (EventRegistration registration : registrations) {
            if (!(registration instanceof Registration)) {
                throw new IllegalArgumentException();
            }
            if (isLocal(registration)) {
                executeLocal(serviceName, event, registration, orderKey);
                continue;
            }

            if (eventData == null) {
                eventData = serializationService.toData(event);
            }
            EventEnvelope eventEnvelope = new EventEnvelope(registration.getId(), serviceName, eventData);
            sendEvent(registration.getSubscriber(), eventEnvelope, orderKey);
        }
    }

    @Override
    public void publishRemoteEvent(String serviceName, Collection<EventRegistration> registrations, Object event, int orderKey) {
        if (registrations.isEmpty()) {
            return;
        }
        Data eventData = serializationService.toData(event);
        for (EventRegistration registration : registrations) {
            if (!(registration instanceof Registration)) {
                throw new IllegalArgumentException();
            }
            if (isLocal(registration)) {
                continue;
            }
            EventEnvelope eventEnvelope = new EventEnvelope(registration.getId(), serviceName, eventData);
            sendEvent(registration.getSubscriber(), eventEnvelope, orderKey);
        }
    }

    private void executeLocal(String serviceName, Object event, EventRegistration registration, int orderKey) {
        if (nodeEngine.isRunning()) {
            Registration reg = (Registration) registration;
            try {
                if (reg.getListener() != null) {
                    eventExecutor.execute(new LocalEventDispatcher(this, serviceName, event, reg.getListener()
                            , orderKey, eventQueueTimeoutMs));
                } else {
                    logger.warning("Something seems wrong! Listener instance is null! -> " + reg);
                }
            } catch (RejectedExecutionException e) {
                rejectedCount.inc();

                if (eventExecutor.isLive()) {
                    logFailure("EventQueue overloaded! %s failed to publish to %s:%s",
                            event, reg.getServiceName(), reg.getTopic());
                }
            }
        }
    }

    private void sendEvent(Address subscriber, EventEnvelope eventEnvelope, int orderKey) {
        final String serviceName = eventEnvelope.getServiceName();
        final EventServiceSegment segment = getSegment(serviceName, true);
        boolean sync = segment.incrementPublish() % eventSyncFrequency == 0;

        if (sync) {
            SendEventOperation op = new SendEventOperation(eventEnvelope, orderKey);
            Future f = nodeEngine.getOperationService()
                    .createInvocationBuilder(serviceName, op, subscriber)
                    .setTryCount(SEND_RETRY_COUNT).invoke();
            try {
                f.get(SEND_EVENT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (Exception e) {
                syncDeliveryFailureCount.inc();
                if (logger.isFinestEnabled()) {
                    logger.finest("Sync event delivery failed. Event: " + eventEnvelope, e);
                }
            }
        } else {
            Packet packet = new Packet(serializationService.toBytes(eventEnvelope), orderKey)
                    .setPacketType(Packet.Type.EVENT);

            if (!nodeEngine.getNode().getConnectionManager().transmit(packet, subscriber)) {
                if (nodeEngine.isRunning()) {
                    logFailure("Failed to send event packet to: %s , connection might not alive.", subscriber);
                }
            }
        }
    }

    public EventServiceSegment getSegment(String service, boolean forceCreate) {
        EventServiceSegment segment = segments.get(service);
        if (segment == null && forceCreate) {
            // we can't make use of the ConcurrentUtil; we need to register the segment to the metricsRegistry in case of creation
            EventServiceSegment newSegment = new EventServiceSegment(service, nodeEngine.getService(service));
            EventServiceSegment existingSegment = segments.putIfAbsent(service, newSegment);
            if (existingSegment == null) {
                segment = newSegment;
                nodeEngine.getMetricsRegistry().scanAndRegister(newSegment, "event.[" + service + "]");
            } else {
                segment = existingSegment;
            }
        }
        return segment;
    }

    boolean isLocal(EventRegistration reg) {
        return nodeEngine.getThisAddress().equals(reg.getSubscriber());
    }

    @Override
    public void executeEventCallback(Runnable callback) {
        if (nodeEngine.isRunning()) {
            try {
                eventExecutor.execute(callback);
            } catch (RejectedExecutionException e) {
                rejectedCount.inc();

                if (eventExecutor.isLive()) {
                    logFailure("EventQueue overloaded! Failed to execute event callback: %s", callback);
                }
            }
        }
    }

    @Override
    public void handle(Packet packet) {
        try {
            eventExecutor.execute(new RemoteEventProcessor(this, packet));
        } catch (RejectedExecutionException e) {
            rejectedCount.inc();

            if (eventExecutor.isLive()) {
                Connection conn = packet.getConn();
                String endpoint = conn.getEndPoint() != null ? conn.getEndPoint().toString() : conn.toString();
                logFailure("EventQueue overloaded! Failed to process event packet sent from: %s", endpoint);
            }
        }
    }

    public PostJoinRegistrationOperation getPostJoinOperation() {
        final Collection<Registration> registrations = new LinkedList<Registration>();
        for (EventServiceSegment segment : segments.values()) {
            //todo: this should be moved into the Segment.
            for (Registration reg : (Iterable<Registration>) segment.getRegistrationIdMap().values()) {
                if (!reg.isLocalOnly()) {
                    registrations.add(reg);
                }
            }
        }
        return registrations.isEmpty() ? null : new PostJoinRegistrationOperation(registrations);
    }

    public void shutdown() {
        logger.finest("Stopping event executor...");
        eventExecutor.shutdown();
        for (EventServiceSegment segment : segments.values()) {
            segment.clear();
        }
        segments.clear();
    }

    public void onMemberLeft(MemberImpl member) {
        final Address address = member.getAddress();
        for (EventServiceSegment segment : segments.values()) {
            segment.onMemberLeft(address);
        }
    }

    private void logFailure(String message, Object... args) {
        totalFailures.inc();

        long total = totalFailures.get();

        // it can happen that 2 threads at the same conclude that the level should be warn because of the
        // non atomic int/get. This is an acceptable trade of since it is unlikely to happen and you only get
        // additional warning under log as a side effect.
        Level level = total % WARNING_LOG_FREQUENCY == 0
                ? Level.WARNING : Level.FINEST;

        if (logger.isLoggable(level)) {
            logger.log(level, String.format(message, args));
        }
    }

}
