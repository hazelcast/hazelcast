/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.jmx;

import com.hazelcast.core.AtomicNumber;

/**
 * MBean for AtomicNumber
 *
 * @author Vladimir Dolzhenko, vladimir.dolzhenko@gmail.com
 */
@JMXDescription("A distributed AtomicLong")
public class AtomicNumberMBean extends AbstractMBean<AtomicNumber> {

    public AtomicNumberMBean(AtomicNumber managedObject, ManagementService managementService) {
        super(managedObject, managementService);
    }

    @Override
    public ObjectNameSpec getNameSpec() {
        return getParentName().getNested("AtomicLong", getName());
    }

    @JMXAttribute("ActualValue")
    @JMXDescription("get() result")
    public long getActualValue() {
        return get();
    }

    @JMXAttribute("Name")
    @JMXDescription("Instance name of the atomic long")
    public String getName() {
        return getManagedObject().getName();
    }

    @JMXOperation("get")
    @JMXDescription("return value")
    public long get() {
        return getManagedObject().get();
    }

    @JMXOperation("set")
    @JMXDescription("set value")
    public void set(final long newValue) {
        getManagedObject().set(newValue);
    }

    @JMXOperation("add")
    @JMXDescription("add value and return")
    public void add(final long delta) {
        getManagedObject().addAndGet(delta);
    }

    @JMXOperation("getAndAdd")
    @JMXDescription("add value return original")
    public long getAdd(final long delta) {
        return getManagedObject().addAndGet(delta);
    }

    @JMXOperation("getAndSet")
    @JMXDescription("set value return original")
    public long getSet(final long delta) {
        return getManagedObject().addAndGet(delta);
    }

    @JMXOperation("increment")
    @JMXDescription("add 1 and return")
    public void incrementAndGet() {
        getManagedObject().incrementAndGet();
    }

    @JMXOperation("decrement")
    @JMXDescription("subtract 1 and return")
    public void decrementAndGet() {
        getManagedObject().decrementAndGet();
    }

    @JMXOperation("reset")
    @JMXDescription("reset value to 0")
    public void reset() {
        getManagedObject().set(0L);
    }

    @JMXOperation("compareAndSet")
    @JMXDescription("if expected set value")
    public void compareAndSet(final long expectedValue, final long newValue) {
        getManagedObject().compareAndSet(expectedValue, newValue);
    }
}
