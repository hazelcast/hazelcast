/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.ListenerWrapperEventFilter;
import com.hazelcast.spi.NotifiableEventListener;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class EventServiceSegment<S> {

    private final String serviceName;
    private final S service;

    private final ConcurrentMap<String, Collection<Registration>> registrations
            = new ConcurrentHashMap<String, Collection<Registration>>();
    private final ConcurrentMap<String, Registration> registrationIdMap
            = new ConcurrentHashMap<String, Registration>();
    private final AtomicLong totalPublishes = new AtomicLong();

    public EventServiceSegment(String serviceName, S service) {
        this.serviceName = serviceName;
        this.service = service;
    }

    private void pingNotifiableEventListenerIfAvailable(String topic, Registration registration, boolean register) {
        Object listener = registration.getListener();
        if (!(listener instanceof NotifiableEventListener)) {
            EventFilter filter = registration.getFilter();
            if (filter instanceof ListenerWrapperEventFilter) {
                listener = ((ListenerWrapperEventFilter) filter).getListener();
            }
        }
        if (listener instanceof NotifiableEventListener) {
            NotifiableEventListener notifiableEventListener = (NotifiableEventListener) listener;
            if (register) {
                notifiableEventListener.onRegister(service, serviceName, topic, registration);
            } else {
                notifiableEventListener.onDeregister(service, serviceName, topic, registration);
            }
        }
    }

    public Collection<Registration> getRegistrations(String topic, boolean forceCreate) {
        Collection<Registration> listenerList = registrations.get(topic);
        if (listenerList == null && forceCreate) {
            ConstructorFunction<String, Collection<Registration>> func
                    = new ConstructorFunction<String, Collection<Registration>>() {
                public Collection<Registration> createNew(String key) {
                    return Collections.newSetFromMap(new ConcurrentHashMap<Registration, Boolean>());
                }
            };
            return ConcurrencyUtil.getOrPutIfAbsent(registrations, topic, func);
        }
        return listenerList;
    }

    public ConcurrentMap<String, Registration> getRegistrationIdMap() {
        return registrationIdMap;
    }

    public boolean addRegistration(String topic, Registration registration) {
        final Collection<Registration> registrations = getRegistrations(topic, true);
        if (registrations.add(registration)) {
            registrationIdMap.put(registration.getId(), registration);
            pingNotifiableEventListenerIfAvailable(topic, registration, true);
            return true;
        }
        return false;
    }

    public Registration removeRegistration(String topic, String id) {
        final Registration registration = registrationIdMap.remove(id);
        if (registration != null) {
            final Collection<Registration> all = registrations.get(topic);
            if (all != null) {
                all.remove(registration);
            }
            pingNotifiableEventListenerIfAvailable(topic, registration, false);
        }
        return registration;
    }

    void removeRegistrations(String topic) {
        final Collection<Registration> all = registrations.remove(topic);
        if (all != null) {
            for (Registration reg : all) {
                registrationIdMap.remove(reg.getId());
                pingNotifiableEventListenerIfAvailable(topic, reg, false);
            }
        }
    }

    void clear() {
        for (Collection<Registration> all : registrations.values()) {
            Iterator<Registration> iter = all.iterator();
            while (iter.hasNext()) {
                Registration reg = iter.next();
                iter.remove();
                registrationIdMap.remove(reg.getId());
                pingNotifiableEventListenerIfAvailable(reg.getTopic(), reg, false);
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
                    pingNotifiableEventListenerIfAvailable(reg.getTopic(), reg, false);
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
}
