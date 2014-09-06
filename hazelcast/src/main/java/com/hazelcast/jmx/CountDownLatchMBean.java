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

import com.hazelcast.core.ICountDownLatch;

/**
 * Management bean for {@link com.hazelcast.core.ICountDownLatch}
 */
@ManagedDescription("ICountDownLatch")
public class CountDownLatchMBean extends HazelcastMBean<ICountDownLatch> {

    protected CountDownLatchMBean(ICountDownLatch managedObject, ManagementService service) {
        super(managedObject, service);
        objectName = service.createObjectName("ICountDownLatch", managedObject.getName());
    }

    @ManagedAnnotation("name")
    @ManagedDescription("")
    public String name() {
        return managedObject.getName();
    }

    @ManagedAnnotation("count")
    @ManagedDescription("Current Count")
    public int getCount() {
        return managedObject.getCount();
    }

    @ManagedAnnotation(value = "countDown", operation = true)
    @ManagedDescription("perform a countdown operation")
    public void countDown() {
        managedObject.countDown();
    }

    @ManagedAnnotation("partitionKey")
    @ManagedDescription("the partitionKey")
    public String getPartitionKey() {
        return managedObject.getPartitionKey();
    }
}
