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

import com.hazelcast.core.IList;
import com.hazelcast.core.ItemListener;

import java.util.ArrayList;
import java.util.List;

/**
 * MBean for Collections
 *
 * @author Marco Ferrante, DISI - University of Genoa
 */
@JMXDescription("A distributed List")
public class ListMBean extends AbstractMBean<IList<?>> {

    @SuppressWarnings("unchecked")
    protected ItemListener listener;

    private StatisticsCollector receivedStats = null;
    private StatisticsCollector servedStats = null;

    public ListMBean(IList<?> managedObject) {
        super(managedObject);
    }

    @Override
    public ObjectNameSpec getNameSpec() {
        return getParentName().getNested("List", getName());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void postRegister(Boolean registrationDone) {
        super.postRegister(registrationDone);
        if (!registrationDone) {
            return;
        }
        if (ManagementService.showDetails()) {
            receivedStats = ManagementService.newStatisticsCollector();
            servedStats = ManagementService.newStatisticsCollector();
            listener = new ItemListener() {

                public void itemAdded(Object item) {
                    receivedStats.addEvent();
                    addItem(item);
                }

                public void itemRemoved(Object item) {
                    servedStats.addEvent();
                    removeItem(item);
                }
            };
            getManagedObject().addItemListener(listener, false);
            // Add existing entries
            for (Object item : getManagedObject()) {
                addItem(item);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void preDeregister() throws Exception {
        if (listener != null) {
            getManagedObject().removeItemListener(listener);
            listener = null;
        }
        if (receivedStats != null) {
            receivedStats.destroy();
            receivedStats = null;
        }
        if (servedStats != null) {
            servedStats.destroy();
            servedStats = null;
        }
        super.preDeregister();
    }

    protected void addItem(Object item) {
        // Manage items?
    }

    protected void removeItem(Object item) {
        // Manage items?
    }

    /**
     * Resets statistics
     */
    @JMXOperation("resetStats")
    public void resetStats() {
        if (receivedStats != null)
            receivedStats.reset();
        if (servedStats != null)
            servedStats.reset();
    }

    @JMXAttribute("Name")
    @JMXDescription("Registration name of the list")
    public String getName() {
        return getManagedObject().getName();
    }

    @JMXAttribute("Size")
    @JMXDescription("Current size")
    public int getSize() {
        return getManagedObject().size();
    }

    @SuppressWarnings("unchecked")
    @JMXAttribute("Items")
    @JMXDescription("Current items")
    public List<?> getItems() {
        ArrayList result = new ArrayList();
        for (Object item : getManagedObject()) {
            result.add(item);
        }
        return result;
    }

    @JMXAttribute("ObjectAdded")
    @JMXDescription("Object added to the util since the start time")
    public long getItemsReceived() {
        return receivedStats.getTotal();
    }

    @JMXAttribute("ObjectRemoved")
    @JMXDescription("Object removed from the util since the start time")
    public long getItemsServed() {
        return servedStats.getTotal();
    }
}
