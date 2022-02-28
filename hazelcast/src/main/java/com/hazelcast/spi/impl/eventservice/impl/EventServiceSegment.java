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
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.services.ListenerWrapperEventFilter;
import com.hazelcast.internal.services.NotifiableEventListener;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.spi.impl.eventservice.EventFilter;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EVENT_METRIC_EVENT_SERVICE_SEGMENT_LISTENER_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EVENT_METRIC_EVENT_SERVICE_SEGMENT_PUBLICATION_COUNT;
import static java.util.Collections.newSetFromMap;

/**
 * Segment of the event service. Each segment is responsible for a single service and
 * holds {@link Registration}s for that service. The segment is responsible for keeping the
 * number of publications and keeping the registrations.
 * The listener registration defines a specific topic that allows for further granulation of
 * the published events. This allows events to be published for the same service but for
 * a different topic (e.g. for a map service the topics are specific map instance names).
 *
 * @param <S> the service type for which this segment is responsible
 */
public class EventServiceSegment<S> {
    /**
     * The name of the service for which this segment is responsible
     */
    private final String serviceName;
    /**
     * The service for which this segment is responsible
     */
    private final S service;

    /**
     * Map of {@link Registration}s grouped by event topic
     */
    private final ConcurrentMap<String, Collection<Registration>> registrations
            = new ConcurrentHashMap<>();

    /**
     * Registration ID to registration map
     */
    @Probe(name = EVENT_METRIC_EVENT_SERVICE_SEGMENT_LISTENER_COUNT)
    private final ConcurrentMap<UUID, Registration> registrationIdMap = new ConcurrentHashMap<>();

    @Probe(name = EVENT_METRIC_EVENT_SERVICE_SEGMENT_PUBLICATION_COUNT)
    private final AtomicLong totalPublishes = new AtomicLong();

    public EventServiceSegment(String serviceName, S service) {
        this.serviceName = serviceName;
        this.service = service;
    }

    /**
     * Notifies the registration of a event in the listener lifecycle.
     * The registration is checked for a {@link NotifiableEventListener} in this order :
     * <ul>
     * <li>first check if {@link Registration#getListener()} returns a {@link NotifiableEventListener}</li>
     * <li>otherwise check if the event filter wraps a listener and use that one</li>
     * </ul>
     *
     * @param topic        the event topic
     * @param registration the listener registration
     * @param register     if the listener was registered or not
     */
    private void pingNotifiableEventListener(String topic, Registration registration, boolean register) {
        Object listener = registration.getListener();
        if (!(listener instanceof NotifiableEventListener)) {
            EventFilter filter = registration.getFilter();
            if (filter instanceof ListenerWrapperEventFilter) {
                listener = ((ListenerWrapperEventFilter) filter).getListener();
            }
        }
        pingNotifiableEventListenerInternal(listener, topic, registration, register);
        pingNotifiableEventListenerInternal(service, topic, registration, register);
    }

    /**
     * Notifies the object of an event in the lifecycle of the listener. The listener may have
     * been registered or deregistered. The object must implement {@link NotifiableEventListener} if
     * it wants to be notified.
     *
     * @param object       the object to notified. It must implement {@link NotifiableEventListener} to be notified
     * @param topic        the event topic name
     * @param registration the listener registration
     * @param register     whether the listener was registered or not
     */
    private void pingNotifiableEventListenerInternal(Object object, String topic, Registration registration, boolean register) {
        if (!(object instanceof NotifiableEventListener)) {
            return;
        }

        NotifiableEventListener listener = ((NotifiableEventListener) object);
        if (register) {
            listener.onRegister(service, serviceName, topic, registration);
        } else {
            listener.onDeregister(service, serviceName, topic, registration);
        }
    }

    /**
     * Returns the {@link Registration}s for the event {@code topic}. If there are no
     * registrations and {@code forceCreate}, it will create a concurrent set and put it in the registration map.
     *
     * @param topic       the event topic
     * @param forceCreate whether to create the registration set if none exists or to return null
     * @return the collection of registrations for the topic or null if none exists and {@code forceCreate} is {@code false}
     */
    public Collection<Registration> getRegistrations(@Nonnull String topic, boolean forceCreate) {
        Collection<Registration> listenerList = registrations.get(topic);
        if (listenerList == null && forceCreate) {
            ConstructorFunction<String, Collection<Registration>> func
                    = key -> newSetFromMap(new ConcurrentHashMap<Registration, Boolean>());
            return ConcurrencyUtil.getOrPutIfAbsent(registrations, topic, func);
        }
        return listenerList;
    }

    /**
     * Returns the map from registration ID to the listener registration.
     */
    public ConcurrentMap<UUID, Registration> getRegistrationIdMap() {
        return registrationIdMap;
    }

    // this method is only used for testing purposes
    public ConcurrentMap<String, Collection<Registration>> getRegistrations() {
        return registrations;
    }

    /**
     * Adds a registration for the {@code topic} and notifies the listener and service of the listener
     * registration. Returns if the registration was added. The registration might not be added
     * if an equal instance is already registered.
     *
     * @param topic        the event topic
     * @param registration the listener registration
     * @return true if the registration is added
     */
    public boolean addRegistration(@Nonnull String topic, Registration registration) {
        Collection<Registration> registrations = getRegistrations(topic, true);
        if (registrations.add(registration)) {
            registrationIdMap.put(registration.getId(), registration);
            pingNotifiableEventListener(topic, registration, true);
            return true;
        }
        return false;
    }

    /**
     * Removes the registration matching the {@code topic} and {@code ID}.
     * Returns the removed registration or {@code null} if none matched.
     *
     * @param topic the registration topic name
     * @param id    the registration ID
     * @return the registration which was removed or {@code null} if none matched
     */
    public Registration removeRegistration(String topic, UUID id) {
        Registration registration = registrationIdMap.remove(id);
        if (registration != null) {
            final Collection<Registration> all = registrations.get(topic);
            if (all != null) {
                all.remove(registration);
            }
            pingNotifiableEventListener(topic, registration, false);
        }
        return registration;
    }

    /**
     * Removes all registrations for the specified topic and notifies the listeners and
     * service of the listener deregistrations.
     *
     * @param topic the topic for which registrations are removed
     */
    void removeRegistrations(String topic) {
        Collection<Registration> all = registrations.remove(topic);
        if (all == null) {
            return;
        }
        for (Registration reg : all) {
            registrationIdMap.remove(reg.getId());
            pingNotifiableEventListener(topic, reg, false);
        }
    }

    void clear() {
        for (Collection<Registration> all : registrations.values()) {
            Iterator<Registration> iter = all.iterator();
            while (iter.hasNext()) {
                Registration reg = iter.next();
                iter.remove();
                registrationIdMap.remove(reg.getId());
                pingNotifiableEventListener(reg.getTopic(), reg, false);
            }
        }
    }

    void onMemberLeft(Address address) {
        for (Collection<Registration> all : registrations.values()) {
            Iterator<Registration> iter = all.iterator();
            while (iter.hasNext()) {
                Registration reg = iter.next();
                if (address.equals(reg.getSubscriber())) {
                    iter.remove();
                    registrationIdMap.remove(reg.getId());
                    pingNotifiableEventListener(reg.getTopic(), reg, false);
                }
            }
        }
    }

    long incrementPublish() {
        return totalPublishes.incrementAndGet();
    }

    boolean hasRegistration(String topic) {
        Collection<Registration> topicRegistrations = registrations.get(topic);
        return !(topicRegistrations == null || topicRegistrations.isEmpty());
    }

    void collectRemoteRegistrations(Collection<Registration> result) {
        for (Registration registration : registrationIdMap.values()) {
            if (!registration.isLocalOnly()) {
                result.add(registration);
            }
        }
    }
}
