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

import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author ali 2/11/13
 */
@ManagedDescription("IQueue")
public class QueueMBean extends HazelcastMBean<IQueue> {

    private AtomicLong totalAddedItemCount = new AtomicLong();

    private AtomicLong totalRemovedItemCount = new AtomicLong();

    private final String registrationId;

    protected QueueMBean(IQueue managedObject, ManagementService service) {
        super(managedObject, service);
        objectName = createObjectName("Queue", managedObject.getName());
        ItemListener itemListener = new ItemListener() {
            public void itemAdded(ItemEvent item) {
                totalAddedItemCount.incrementAndGet();
            }

            public void itemRemoved(ItemEvent item) {
                totalRemovedItemCount.incrementAndGet();
            }
        };
        registrationId = managedObject.addItemListener(itemListener, false);
    }

    @ManagedAnnotation("name")
    @ManagedDescription("Name of the DistributedObject")
    public String getName() {
        return managedObject.getName();
    }

    @ManagedAnnotation("config")
    @ManagedDescription("QueueConfig")
    public String getConfig(){
        return service.instance.getConfig().getQueueConfig(managedObject.getName()).toString();
    }

    @ManagedAnnotation(value = "clear", operation = true)
    @ManagedDescription("Clear Queue")
    public void clear() {
        managedObject.clear();
    }

    @ManagedAnnotation("totalAddedItemCount")
    public long getTotalAddedItemCount() {
        return totalAddedItemCount.get();
    }

    @ManagedAnnotation("totalRemovedItemCount")
    public long getTotalRemovedItemCount() {
        return totalRemovedItemCount.get();
    }

    public void preDeregister() throws Exception {
        super.preDeregister();
        managedObject.removeItemListener(registrationId);
    }


}
