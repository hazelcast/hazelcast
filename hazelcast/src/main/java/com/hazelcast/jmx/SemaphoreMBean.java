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

import com.hazelcast.core.ISemaphore;

/**
 * Management bean for {@link com.hazelcast.core.ISemaphore}
 */
@ManagedDescription("ISemaphore")
public class SemaphoreMBean extends HazelcastMBean<ISemaphore> {

    protected SemaphoreMBean(ISemaphore managedObject, ManagementService service) {
        super(managedObject, service);
        objectName = service.createObjectName("ISemaphore", managedObject.getName());
    }

    @ManagedAnnotation("name")
    public String getName() {
        return managedObject.getName();
    }

    @ManagedAnnotation("available")
    public int getAvailable() {
        return managedObject.availablePermits();
    }

    @ManagedAnnotation(value = "drain", operation = true)
    @ManagedDescription("Acquire and return all permits that are immediately available")
    public int drain() {
        return managedObject.drainPermits();
    }

    @ManagedAnnotation(value = "reduce", operation = true)
    @ManagedDescription("Shrinks the number of available permits by the indicated reduction. Does not block")
    public void reduce(int reduction) {
        managedObject.reducePermits(reduction);
    }

    @ManagedAnnotation(value = "release", operation = true)
    @ManagedDescription("Releases the given number of permits, increasing the number of available permits by that amount")
    public void release(int permits) {
        managedObject.release(permits);
    }

    @ManagedAnnotation("partitionKey")
    @ManagedDescription("the partitionKey")
    public String getPartitionKey() {
        return managedObject.getPartitionKey();
    }
}
