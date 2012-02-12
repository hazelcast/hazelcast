/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.core.ISemaphore;

/**
 * MBean for ISemaphore
 */
@JMXDescription("A distributed Semaphore")
public class SemaphoreMBean extends AbstractMBean<ISemaphore> {

    public SemaphoreMBean(ISemaphore managedObject, ManagementService managementService) {
        super(managedObject, managementService);
    }

    @Override
    public ObjectNameSpec getNameSpec() {
        return getParentName().getNested("Semaphore", getName());
    }

    @JMXAttribute("CurrentPermits")
    @JMXDescription("availablePermits() result")
    public long getCurrentPermits() {
        return available();
    }

    @JMXAttribute("Name")
    @JMXDescription("Instance name of the Semaphore")
    public String getName() {
        return getManagedObject().getName();
    }

    @JMXOperation("available")
    @JMXDescription("number of permits immediately available")
    public int available() {
        return getManagedObject().availablePermits();
    }

    @JMXOperation("drain")
    @JMXDescription("acquire and return all permits immediately available")
    public int drain() {
        return getManagedObject().drainPermits();
    }

    @JMXOperation("reduce")
    @JMXDescription("reduce the number of permits available")
    public void reduce(int reduction) {
        getManagedObject().reducePermits(reduction);
    }

    @JMXOperation("release")
    @JMXDescription("increase the number of permits available")
    public void release(int permits) {
        getManagedObject().release(permits);
    }
}
