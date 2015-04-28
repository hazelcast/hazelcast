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

import com.hazelcast.internal.blackbox.SensorInput;
import com.hazelcast.nio.Address;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class EventServiceSegment {
    private final String serviceName;

    private final ConcurrentMap<String, Collection<Registration>> registrations
            = new ConcurrentHashMap<String, Collection<Registration>>();

    @SensorInput(name = "listenerCount")
    private final ConcurrentMap<String, Registration> registrationIdMap = new ConcurrentHashMap<String, Registration>();

    @SensorInput(name = "publicationCount")
    private final AtomicLong totalPublishes = new AtomicLong();

    public EventServiceSegment(String serviceName) {
        this.serviceName = serviceName;
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
        }
        return registration;
    }

    void removeRegistrations(String topic) {
        final Collection<Registration> all = registrations.remove(topic);
        if (all != null) {
            for (Registration reg : all) {
                registrationIdMap.remove(reg.getId());
            }
        }
    }

    void clear() {
        registrations.clear();
        registrationIdMap.clear();
    }

    void onMemberLeft(Address address) {
        for (Collection<Registration> all : registrations.values()) {
            Iterator<Registration> iter = all.iterator();
            while (iter.hasNext()) {
                Registration reg = iter.next();
                if (address.equals(reg.getSubscriber())) {
                    iter.remove();
                    registrationIdMap.remove(reg.getId());
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
