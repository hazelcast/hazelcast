/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jmx;

import com.hazelcast.core.IAtomicLong;

/**
 * Management bean for {@link com.hazelcast.core.IAtomicLong}
 */
@ManagedDescription("IAtomicLong")
public class AtomicLongMBean extends HazelcastMBean<IAtomicLong> {

    public AtomicLongMBean(IAtomicLong managedObject, ManagementService service) {
        super(managedObject, service);
        objectName = service.createObjectName("IAtomicLong", managedObject.getName());
    }

    @ManagedAnnotation("name")
    @ManagedDescription("Name of the DistributedObject")
    public String getName() {
        return managedObject.getName();
    }

    @ManagedAnnotation("currentValue")
    @ManagedDescription("Current Value")
    public long getCurrentValue() {
        return managedObject.get();
    }

    @ManagedAnnotation(value = "set", operation = true)
    @ManagedDescription("set value")
    public void set(long value) {
        managedObject.set(value);
    }

    @ManagedAnnotation(value = "addAndGet", operation = true)
    @ManagedDescription("add value and get")
    public long addAndGet(long delta) {
        return managedObject.addAndGet(delta);
    }

    @ManagedAnnotation(value = "compareAndSet", operation = true)
    @ManagedDescription("compare expected value with current value if equals then set")
    public boolean compareAndSet(long expect, long value) {
        return managedObject.compareAndSet(expect, value);
    }

    @ManagedAnnotation(value = "decrementAndGet", operation = true)
    @ManagedDescription("decrement the current value and get")
    public long decrementAndGet() {
        return managedObject.decrementAndGet();
    }

    @ManagedAnnotation(value = "getAndAdd", operation = true)
    @ManagedDescription("get the current value then add")
    public long getAndAdd(long delta) {
        return managedObject.getAndAdd(delta);
    }

    @ManagedAnnotation(value = "getAndIncrement", operation = true)
    @ManagedDescription("get the current value then increment")
    public long getAndIncrement() {
        return managedObject.getAndIncrement();
    }

    @ManagedAnnotation(value = "getAndSet", operation = true)
    @ManagedDescription("get the current value then set")
    public long getAndSet(long value) {
        return managedObject.getAndSet(value);
    }

    @ManagedAnnotation(value = "incrementAndGet", operation = true)
    @ManagedDescription("increment the current value then get")
    public long incrementAndGet() {
        return managedObject.incrementAndGet();
    }

    @ManagedAnnotation("partitionKey")
    @ManagedDescription("the partitionKey")
    public String getPartitionKey() {
        return managedObject.getPartitionKey();
    }

}
