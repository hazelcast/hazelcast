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
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.StaticMetricsProvider;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.internal.util.executor.StripedExecutor;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.EventFilter;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.eventservice.impl.operations.DeregistrationOperationSupplier;
import com.hazelcast.spi.impl.eventservice.impl.operations.OnJoinRegistrationOperation;
import com.hazelcast.spi.impl.eventservice.impl.operations.RegistrationOperationSupplier;
import com.hazelcast.spi.impl.eventservice.impl.operations.SendEventOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.properties.HazelcastProperties;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Supplier;
import java.util.logging.Level;

import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EVENT_DISCRIMINATOR_SERVICE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EVENT_METRIC_EVENT_SERVICE_EVENTS_PROCESSED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EVENT_METRIC_EVENT_SERVICE_EVENT_QUEUE_SIZE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EVENT_METRIC_EVENT_SERVICE_QUEUE_CAPACITY;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EVENT_METRIC_EVENT_SERVICE_REJECTED_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EVENT_METRIC_EVENT_SERVICE_SYNC_DELIVERY_FAILURE_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EVENT_METRIC_EVENT_SERVICE_THREAD_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EVENT_METRIC_EVENT_SERVICE_TOTAL_FAILURE_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EVENT_PREFIX;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.FutureUtil.getValue;
import static com.hazelcast.internal.util.InvocationUtil.invokeOnStableClusterSerial;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.ThreadUtil.createThreadName;
import static com.hazelcast.internal.util.counters.MwCounter.newMwCounter;
import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;
import static com.hazelcast.spi.properties.ClusterProperty.EVENT_QUEUE_CAPACITY;
import static com.hazelcast.spi.properties.ClusterProperty.EVENT_QUEUE_TIMEOUT_MILLIS;
import static com.hazelcast.spi.properties.ClusterProperty.EVENT_SYNC_TIMEOUT_MILLIS;
import static com.hazelcast.spi.properties.ClusterProperty.EVENT_THREAD_COUNT;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Service responsible for routing and dispatching local and remote events and keeping track of listener
 * registrations. Local events are events published on a local subscriber (the subscriber is on this node)
 * and remote events are published on a remote subscriber (the subscriber is on a different node and the
 * event is sent to that node). The remote events are generally asnychronous meaning that we send the event
 * and don't wait for the response. The exception to this is that every {@link #eventSyncFrequency} remote
 * event is sent as an operation and we wait for it to be submitted to the remote queue.
 * <p>
 * This implementation keeps registrations grouped into {@link EventServiceSegment}s. Each segment is
 * responsible for a single service (e.g. map service, cluster service, proxy service).
 * <p>
 * The events are processed on a {@link StripedExecutor}. The executor has a fixed queue and thread size
 * and it is shared between all events meaning that it is important to configure it correctly. Inadequate thread
 * count sizing can lead to wasted threads or low throughput. Inadequate queue size can lead
 * to {@link OutOfMemoryError} or events being dropped when the queue is full.
 * The events are ordered by order key which can be defined when publishing the event meaning that you can
 * define your custom ordering. Events with the same order key will be processed by the same thread on
 * the executor.
 * <p>
 * This order can still be broken in some cases. This is possible because remote events are asynchronous
 * and we don't wait for the response before publishing the next event. The previously published
 * event can be retransmitted causing it to be received by the target node at a later time.
 */
@SuppressWarnings({"checkstyle:classfanoutcomplexity", "checkstyle:methodcount"})
public class EventServiceImpl implements EventService, StaticMetricsProvider {

    public static final String SERVICE_NAME = "hz:core:eventService";

    /**
     * Usually remote events are sent asynchronously. This property dictates how often the event is sent
     * synchronously. This means that the event will be sent as a {@link SendEventOperation} and we will
     * wait for the response. The default value is {@value EVENT_SYNC_FREQUENCY}.
     *
     * @see #sendEvent(Address, EventEnvelope, int)
     */
    public static final String EVENT_SYNC_FREQUENCY_PROP = "hazelcast.event.sync.frequency";

    private static final EventRegistration[] EMPTY_REGISTRATIONS = new EventRegistration[0];

    /**
     * The default value for the {@link #EVENT_SYNC_FREQUENCY_PROP}.
     *
     * @see #sendEvent(Address, EventEnvelope, int)
     */
    private static final int EVENT_SYNC_FREQUENCY = 100000;
    /**
     * The retry count for the synchronous remote events.
     *
     * @see #sendEvent(Address, EventEnvelope, int)
     */
    private static final int SEND_RETRY_COUNT = 50;
    /**
     * How often failures are logged with {@link Level#WARNING}. Otherwise the failures are
     * logged with a lower log level.
     */
    private static final int WARNING_LOG_FREQUENCY = 1000;
    /**
     * Retry count for registration & deregistration operation invocations.
     */
    private static final int MAX_RETRIES = 100;


    final ILogger logger;
    final NodeEngineImpl nodeEngine;

    /** Service name to event service segment map */
    private final ConcurrentMap<String, EventServiceSegment> segments;
    /** The executor responsible for processing events */
    private final StripedExecutor eventExecutor;
    /**
     * The timeout in milliseconds for offering an event to the local executor for processing. If the queue is full
     * and the event is not accepted in the defined timeout, it will not be processed.
     * This applies only to processing local events. Remote events (events on a remote subscriber) have no timeout,
     * meaning that the event can be rejected immediately.
     */
    private final long eventQueueTimeoutMs;

    /** The thread count for the executor processing the events. */
    @Probe(name = EVENT_METRIC_EVENT_SERVICE_THREAD_COUNT)
    private final int eventThreadCount;
    /** The capacity of the executor processing the events. This capacity is shared for all events. */
    @Probe(name = EVENT_METRIC_EVENT_SERVICE_QUEUE_CAPACITY)
    private final int eventQueueCapacity;
    @Probe(name = EVENT_METRIC_EVENT_SERVICE_TOTAL_FAILURE_COUNT)
    private final MwCounter totalFailures = newMwCounter();
    @Probe(name = EVENT_METRIC_EVENT_SERVICE_REJECTED_COUNT)
    private final MwCounter rejectedCount = newMwCounter();
    @Probe(name = EVENT_METRIC_EVENT_SERVICE_SYNC_DELIVERY_FAILURE_COUNT)
    private final MwCounter syncDeliveryFailureCount = newMwCounter();

    private final int sendEventSyncTimeoutMillis;

    private final InternalSerializationService serializationService;
    private final int eventSyncFrequency;

    public EventServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
        this.logger = nodeEngine.getLogger(EventService.class.getName());
        HazelcastProperties hazelcastProperties = nodeEngine.getProperties();
        this.eventThreadCount = hazelcastProperties.getInteger(EVENT_THREAD_COUNT);
        this.eventQueueCapacity = hazelcastProperties.getInteger(EVENT_QUEUE_CAPACITY);
        this.eventQueueTimeoutMs = hazelcastProperties.getMillis(EVENT_QUEUE_TIMEOUT_MILLIS);
        this.sendEventSyncTimeoutMillis = hazelcastProperties.getInteger(EVENT_SYNC_TIMEOUT_MILLIS);
        this.eventSyncFrequency = loadEventSyncFrequency();

        this.eventExecutor = new StripedExecutor(
                nodeEngine.getNode().getLogger(EventServiceImpl.class),
                createThreadName(nodeEngine.getHazelcastInstance().getName(), "event"),
                eventThreadCount,
                eventQueueCapacity);
        this.segments = new ConcurrentHashMap<>();
    }


    private static int loadEventSyncFrequency() {
        try {
            int eventSyncFrequency = Integer.parseInt(System.getProperty(EVENT_SYNC_FREQUENCY_PROP));
            if (eventSyncFrequency <= 0) {
                eventSyncFrequency = EVENT_SYNC_FREQUENCY;
            }
            return eventSyncFrequency;
        } catch (Exception e) {
            return EVENT_SYNC_FREQUENCY;
        }
    }

    @Override
    public void provideStaticMetrics(MetricsRegistry registry) {
        registry.registerStaticMetrics(this, EVENT_PREFIX);
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
            ignore(e);
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

    @Probe(name = EVENT_METRIC_EVENT_SERVICE_EVENT_QUEUE_SIZE, level = MANDATORY)
    @Override
    public int getEventQueueSize() {
        return eventExecutor.getWorkQueueSize();
    }

    @Probe(name = EVENT_METRIC_EVENT_SERVICE_EVENTS_PROCESSED, level = MANDATORY)
    private long eventsProcessed() {
        return eventExecutor.processedCount();
    }

    @Override
    public EventRegistration registerLocalListener(@Nonnull String serviceName,
                                                   @Nonnull String topic,
                                                   @Nonnull Object listener) {
        return registerLocalListener(serviceName, topic, TrueEventFilter.INSTANCE, listener);
    }

    @Override
    public EventRegistration registerLocalListener(@Nonnull String serviceName, @Nonnull String topic,
                                                   @Nonnull EventFilter filter, @Nonnull Object listener) {
        return registerListener0(serviceName, topic, filter, listener, true);
    }

    /**
     *
     /**
     * Registers the listener for events matching the service name, topic and filter on local member.
     *
     * @param serviceName the service name for which we are registering
     * @param topic       the event topic for which we are registering
     * @param filter      the filter for the listened events
     * @param listener    the event listener
     * @return the event registration
     * @throws IllegalArgumentException if the listener or filter is null
     */
    private EventRegistration registerListener0(@Nonnull String serviceName, @Nonnull String topic,
                                                @Nonnull EventFilter filter, @Nonnull Object listener,
                                                boolean isLocal) {
        checkNotNull(listener, "Null listener is not allowed!");
        checkNotNull(filter, "Null filter is not allowed!");
        EventServiceSegment segment = getSegment(serviceName, true);
        UUID id = UuidUtil.newUnsecureUUID();
        final Registration reg = new Registration(id, serviceName, topic, filter, nodeEngine.getThisAddress(), listener, isLocal);
        if (!segment.addRegistration(topic, reg)) {
            return null;
        }

        return reg;
    }

    @Override
    public EventRegistration registerListener(@Nonnull String serviceName,
                                              @Nonnull String topic,
                                              @Nonnull Object listener) {
        return getValue(registerListenerAsync(serviceName, topic, listener));
    }

    @Override
    public EventRegistration registerListener(@Nonnull String serviceName,
                                              @Nonnull String topic,
                                              @Nonnull EventFilter filter,
                                              @Nonnull Object listener) {
        return getValue(registerListenerAsync(serviceName, topic, filter, listener));
    }

    /**
     * Registers the listener for events matching the service name, topic and filter.
     * It will register only for events published on this node and then the registration is sent to other nodes and the listener
     * will listen for events on all cluster members.
     *
     * @param serviceName the service name for which we are registering
     * @param topic       the event topic for which we are registering
     * @param listener    the event listener
     * @return the event registration future
     */
    @Override
    public CompletableFuture<EventRegistration> registerListenerAsync(@Nonnull String serviceName, @Nonnull String topic,
                                                                      @Nonnull Object listener) {
        return registerListenerAsync(serviceName, topic, TrueEventFilter.INSTANCE, listener);
    }

    /**
     * Registers the listener for events matching the service name, topic and filter.
     * It will register only for events published on this node and then the registration is sent to other nodes and the listener
     * will listen for events on all cluster members.
     *
     * @param serviceName the service name for which we are registering
     * @param topic       the event topic for which we are registering
     * @param filter      the filter for the listened events
     * @param listener    the event listener
     * @return the event registration future
     */
    @Override
    public CompletableFuture<EventRegistration> registerListenerAsync(@Nonnull String serviceName, @Nonnull String topic,
                                                                          @Nonnull EventFilter filter, @Nonnull Object listener) {
        Registration registration = (Registration) registerListener0(serviceName, topic, filter, listener, false);

        if (registration == null) {
            newCompletedFuture(null);
        }

        return invokeOnAllMembers(registration, new RegistrationOperationSupplier(registration, nodeEngine.getClusterService()));
    }

    private CompletableFuture<EventRegistration> invokeOnAllMembers(Registration reg, Supplier<Operation> operationSupplier) {
        // we do not check the result value but always return registration. The method will throw exception if a failure.
        return invokeOnStableClusterSerial(nodeEngine, operationSupplier, MAX_RETRIES)
                .thenApplyAsync(result -> reg, CALLER_RUNS);
    }

    public boolean handleRegistration(Registration reg) {
        if (nodeEngine.getThisAddress().equals(reg.getSubscriber())) {
            return false;
        }
        EventServiceSegment segment = getSegment(reg.getServiceName(), true);
        return segment.addRegistration(reg.getTopic(), reg);
    }

    @Override
    public boolean deregisterListener(@Nonnull String serviceName,
                                      @Nonnull String topic,
                                      @Nonnull Object id) {
        Boolean value = getValue(deregisterListenerAsync(serviceName, topic, id));
        return value != null && value;
    }

    @Override
    public CompletableFuture<Boolean> deregisterListenerAsync(@Nonnull String serviceName,
                                      @Nonnull String topic,
                                      @Nonnull Object id) {
        checkNotNull(serviceName, "Null serviceName is not allowed!");
        checkNotNull(topic, "Null topic is not allowed!");
        checkNotNull(id, "Null id is not allowed!");

        EventServiceSegment segment = getSegment(serviceName, false);
        if (segment == null) {
            return newCompletedFuture(false);
        }

        Registration reg = segment.removeRegistration(topic, (UUID) id);
        if (reg == null) {
            return newCompletedFuture(false);
        }

        if (!reg.isLocalOnly()) {
            return invokeOnAllMembers(reg, new DeregistrationOperationSupplier(reg, nodeEngine.getClusterService()))
                    .thenApplyAsync(Objects::nonNull, CALLER_RUNS);
        } else {
            return newCompletedFuture(true);
        }
    }

    @Override
    public void deregisterAllListeners(@Nonnull String serviceName, @Nonnull String topic) {
        EventServiceSegment segment = getSegment(serviceName, false);
        if (segment != null) {
            segment.removeRegistrations(topic);
        }
    }

    public StripedExecutor getEventExecutor() {
        return eventExecutor;
    }

    @Override
    public EventRegistration[] getRegistrationsAsArray(@Nonnull String serviceName, @Nonnull String topic) {
        EventServiceSegment segment = getSegment(serviceName, false);
        if (segment == null) {
            return EMPTY_REGISTRATIONS;
        }
        Collection<Registration> registrations = segment.getRegistrations(topic, false);
        if (registrations == null || registrations.isEmpty()) {
            return EMPTY_REGISTRATIONS;
        } else {
            return registrations.toArray(new Registration[0]);
        }
    }

    /**
     * {@inheritDoc}
     * The returned collection is unmodifiable and the method always returns a non-null collection.
     *
     * @param serviceName service name
     * @param topic       topic name
     * @return a non-null immutable collection of listener registrations
     */
    @Override
    public Collection<EventRegistration> getRegistrations(@Nonnull String serviceName, @Nonnull String topic) {
        EventServiceSegment segment = getSegment(serviceName, false);
        if (segment == null) {
            return Collections.emptySet();
        }

        Collection<Registration> registrations = segment.getRegistrations(topic, false);
        if (registrations == null || registrations.isEmpty()) {
            return Collections.emptySet();
        } else {
            return Collections.unmodifiableCollection(registrations);
        }
    }

    @Override
    public boolean hasEventRegistration(@Nonnull String serviceName, @Nonnull String topic) {
        EventServiceSegment segment = getSegment(serviceName, false);
        if (segment == null) {
            return false;
        }
        return segment.hasRegistration(topic);
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

    /**
     * {@inheritDoc}
     *
     * @param serviceName   service name
     * @param registrations multiple event registrations
     * @param event         event object
     * @param orderKey      order key
     * @throws IllegalArgumentException if any registration is not an instance of {@link Registration}
     */
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

    /**
     * Processes the {@code event} on this node. If the event is not accepted to the executor
     * in {@link #eventQueueTimeoutMs}, it will be rejected and not processed. This means that we increase the
     * rejected count and log the failure.
     *
     * @param serviceName  the name of the service responsible for this event
     * @param event        the event
     * @param registration the listener registration responsible for this event
     * @param orderKey     the key defining the thread on which the event is processed. Events with the same key maintain order.
     * @see LocalEventDispatcher
     */
    private void executeLocal(String serviceName, Object event, EventRegistration registration, int orderKey) {
        if (!nodeEngine.isRunning()) {
            return;
        }

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

    /**
     * Sends a remote event to the {@code subscriber}.
     * Each event segment keeps track of the published event count. On every {@link #eventSyncFrequency} the event will
     * be sent synchronously.
     * A synchronous event means that we send the event as an {@link SendEventOperation} and in case of failure
     * we increase the failure count and log the failure (see {@link EventProcessor})
     * Otherwise, we send an asynchronous event. This means that we don't wait to see if the processing failed with an
     * exception (see {@link RemoteEventProcessor})
     */
    private void sendEvent(Address subscriber, EventEnvelope eventEnvelope, int orderKey) {
        String serviceName = eventEnvelope.getServiceName();
        EventServiceSegment segment = getSegment(serviceName, true);
        boolean sync = segment.incrementPublish() % eventSyncFrequency == 0;

        if (sync) {
            SendEventOperation op = new SendEventOperation(eventEnvelope, orderKey);
            Future f = nodeEngine.getOperationService()
                    .createInvocationBuilder(serviceName, op, subscriber)
                    .setTryCount(SEND_RETRY_COUNT).invoke();
            try {
                f.get(sendEventSyncTimeoutMillis, MILLISECONDS);
            } catch (Exception e) {
                syncDeliveryFailureCount.inc();
                if (logger.isFinestEnabled()) {
                    logger.finest("Sync event delivery failed. Event: " + eventEnvelope, e);
                }
            }
        } else {
            Packet packet = new Packet(serializationService.toBytes(eventEnvelope), orderKey)
                    .setPacketType(Packet.Type.EVENT);

            ServerConnectionManager cm = nodeEngine.getNode().getServer().getConnectionManager(MEMBER);
            if (!cm.transmit(packet, subscriber)) {
                if (nodeEngine.isRunning()) {
                    logFailure("Failed to send event packet to: %s, connection might not be alive.", subscriber);
                }
            }
        }
    }

    /**
     * Returns the {@link EventServiceSegment} for the {@code service}. If the segment is {@code null} and
     * {@code forceCreate} is {@code true}, the segment is created and registered with the {@link MetricsRegistry}.
     *
     * @param service     the service of the segment
     * @param forceCreate whether the segment should be created in case there is no segment
     * @return the segment for the service or null if there is no segment and {@code forceCreate} is {@code false}
     */
    public EventServiceSegment getSegment(@Nonnull String service, boolean forceCreate) {
        EventServiceSegment segment = segments.get(service);
        if (segment == null && forceCreate) {
            // we can't make use of the ConcurrentUtil; we need to register the segment to the metricsRegistry in case of creation
            EventServiceSegment newSegment = new EventServiceSegment(service, nodeEngine.getService(service));
            EventServiceSegment existingSegment = segments.putIfAbsent(service, newSegment);
            if (existingSegment == null) {
                segment = newSegment;
                MetricsRegistry metricsRegistry = nodeEngine.getMetricsRegistry();
                MetricDescriptor descriptor = metricsRegistry
                        .newMetricDescriptor()
                        .withPrefix(EVENT_PREFIX)
                        .withDiscriminator(EVENT_DISCRIMINATOR_SERVICE, service);
                metricsRegistry.registerStaticMetrics(descriptor, newSegment);
            } else {
                segment = existingSegment;
            }
        }
        return segment;
    }

    /** Returns {@code true} if the subscriber of the registration is this node */
    boolean isLocal(EventRegistration reg) {
        return nodeEngine.getThisAddress().equals(reg.getSubscriber());
    }

    /**
     * {@inheritDoc}
     * If the execution is rejected, the rejection count is increased and a failure is logged.
     * The event callback is not re-executed.
     *
     * @param callback the callback to execute on a random event thread
     */
    @Override
    public void executeEventCallback(@Nonnull Runnable callback) {
        if (!nodeEngine.isRunning()) {
            return;
        }
        try {
            eventExecutor.execute(callback);
        } catch (RejectedExecutionException e) {
            rejectedCount.inc();

            if (eventExecutor.isLive()) {
                logFailure("EventQueue overloaded! Failed to execute event callback: %s", callback);
            }
        }
    }

    /**
     * {@inheritDoc}
     * Handles an asynchronous remote event with a {@link RemoteEventProcessor}. The
     * processor may determine the thread which will handle the event. If the execution is rejected,
     * the rejection count is increased and a failure is logged. The event processing is not retried.
     *
     * @param packet the response packet to handle
     * @see #sendEvent(Address, EventEnvelope, int)
     */
    @Override
    public void accept(Packet packet) {
        try {
            eventExecutor.execute(new RemoteEventProcessor(this, packet));
        } catch (RejectedExecutionException e) {
            rejectedCount.inc();

            if (eventExecutor.isLive()) {
                Connection conn = packet.getConn();
                String endpoint = conn.getRemoteAddress() != null ? conn.getRemoteAddress().toString() : conn.toString();
                logFailure("EventQueue overloaded! Failed to process event packet sent from: %s", endpoint);
            }
        }
    }

    @Override
    public Operation getPreJoinOperation() {
        // pre-join operations are only sent by master member
        return getOnJoinRegistrationOperation();
    }

    @Override
    public Operation getPostJoinOperation() {
        ClusterService clusterService = nodeEngine.getClusterService();
        // Send post join registration operation only if this is the newly joining member.
        // Master will send registrations with pre-join operation.
        return clusterService.isMaster() ? null : getOnJoinRegistrationOperation();
    }

    /**
     * Collects all non-local registrations and returns them as a {@link OnJoinRegistrationOperation}.
     *
     * @return the on join operation containing all non-local registrations
     */
    private OnJoinRegistrationOperation getOnJoinRegistrationOperation() {
        Collection<Registration> registrations = new LinkedList<>();
        for (EventServiceSegment segment : segments.values()) {
            segment.collectRemoteRegistrations(registrations);
        }
        return registrations.isEmpty() ? null : new OnJoinRegistrationOperation(registrations);
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
        Address address = member.getAddress();
        for (EventServiceSegment segment : segments.values()) {
            segment.onMemberLeft(address);
        }
    }

    /**
     * Increase the failure count and log the failure. Every {@value #WARNING_LOG_FREQUENCY} log is logged
     * with a higher log level.
     *
     * @param message the log message
     * @param args    the log message arguments
     */
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
