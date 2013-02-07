/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jmxlocal;

import com.hazelcast.core.IList;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;

/**
 * @ali 1/31/13
 */
@ManagedDescription("IList")
public class ListMBean extends HazelcastMBean<IList<?>> {

    private long totalAddedItemCount;

    private long totalRemovedItemCount;

    protected ListMBean(IList<?> managedObject, ManagementService service) {
        super(managedObject, service);
        objectName = createObjectName("List",managedObject.getName());
        ItemListener itemListener = new ItemListener() {
            public void itemAdded(ItemEvent item) {
                totalAddedItemCount++;
            }

            public void itemRemoved(ItemEvent item) {
                totalRemovedItemCount++;
            }
        };
        managedObject.addItemListener(itemListener, false);
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
    @ManagedDescription("Name of the DistributedObject")
    public long getTotalAddedItemCount() {
        return totalAddedItemCount;
    }

    @ManagedAnnotation("totalRemovedItemCount")
    @ManagedDescription("Name of the DistributedObject")
    public long getTotalRemovedItemCount() {
        return totalRemovedItemCount;
    }

}
