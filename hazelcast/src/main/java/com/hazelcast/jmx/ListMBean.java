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

import com.hazelcast.core.IList;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.util.EmptyStatement.ignore;

/**
 * Management bean for {@link com.hazelcast.core.IList}
 */
@ManagedDescription("IList")
public class ListMBean extends HazelcastMBean<IList<?>> {

    private AtomicLong totalAddedItemCount = new AtomicLong();
    private AtomicLong totalRemovedItemCount = new AtomicLong();
    private final String registrationId;

    protected ListMBean(IList<?> managedObject, ManagementService service) {
        super(managedObject, service);
        objectName = service.createObjectName("IList", managedObject.getName());

        //todo: using the event system to register number of adds/remove is an very expensive price to pay.
        ItemListener itemListener = new ItemListener() {
            @Override
            public void itemAdded(ItemEvent item) {
                totalAddedItemCount.incrementAndGet();
            }

            @Override
            public void itemRemoved(ItemEvent item) {
                totalRemovedItemCount.incrementAndGet();
            }
        };
        registrationId = managedObject.addItemListener(itemListener, false);
    }

    @ManagedAnnotation(value = "clear", operation = true)
    @ManagedDescription("Clear List")
    public void clear() {
        managedObject.clear();
    }

    @ManagedAnnotation("name")
    @ManagedDescription("Name of the DistributedObject")
    public String getName() {
        return managedObject.getName();
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
        try {
            managedObject.removeItemListener(registrationId);
        } catch (Exception ignored) {
            ignore(ignored);
        }
    }
}
