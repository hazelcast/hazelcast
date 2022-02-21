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

package com.hazelcast.internal.jmx;

import com.hazelcast.collection.ISet;

/**
 * Management bean for {@link ISet}
 */
@ManagedDescription("ISet")
public class SetMBean extends HazelcastMBean<ISet> {

    protected SetMBean(ISet managedObject, ManagementService service) {
        super(managedObject, service);
        this.objectName = service.createObjectName("ISet", managedObject.getName());
    }

    @ManagedAnnotation(value = "clear", operation = true)
    @ManagedDescription("Clear Set")
    public void clear() {
        managedObject.clear();
    }

    @ManagedAnnotation("name")
    @ManagedDescription("Name of the DistributedObject")
    public String getName() {
        return managedObject.getName();
    }

    @ManagedAnnotation("partitionKey")
    @ManagedDescription("the partitionKey")
    public String getPartitionKey() {
        return managedObject.getPartitionKey();
    }
}
