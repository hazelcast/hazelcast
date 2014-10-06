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

import com.hazelcast.core.ILock;

/**
 * Management bean for {@link com.hazelcast.core.ILock}
 */
@ManagedDescription("ILock")
public class LockMBean extends HazelcastMBean<ILock> {

    protected LockMBean(ILock managedObject, ManagementService service) {
        super(managedObject, service);
        objectName = service.createObjectName("ILock", managedObject.getName());
    }

    @ManagedAnnotation("name")
    @ManagedDescription("Name of the DistributedObject")
    public String getName() {
        return managedObject.getName();
    }

    @ManagedAnnotation("lockObject")
    @ManagedDescription("Lock Object as String")
    public String getLockObject() {
        Object lockObject = managedObject.getName();
        if (lockObject == null) {
            return null;
        } else {
            return lockObject.toString();
        }
    }

    @ManagedAnnotation("remainingLeaseTime")
    @ManagedDescription("remaining time in milliseconds or -1 if not locked")
    public long getRemainingLeaseTime() {
        return managedObject.getRemainingLeaseTime();
    }

    @ManagedAnnotation("lockCount")
    @ManagedDescription("re-entrant lock hold count, regardless of lock ownership")
    public int getLockCount() {
        return managedObject.getLockCount();
    }

    @ManagedAnnotation(value = "forceUnlock", operation = true)
    @ManagedDescription("force unlock of this lock")
    public void clear() {
        managedObject.forceUnlock();
    }

    @ManagedAnnotation("partitionKey")
    @ManagedDescription("the partitionKey")
    public String getPartitionKey() {
        return managedObject.getPartitionKey();
    }
}
