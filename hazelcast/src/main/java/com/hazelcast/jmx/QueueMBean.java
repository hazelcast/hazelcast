/* 
 * Copyright (c) 2007-2009, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemListener;

/**
 * Management bean for Hazelcst Queue
 * 
 * @author Marco Ferrante, DISI - University of Genoa
 */
@JMXDescription("A distributed queue")
public class QueueMBean extends AbstractMBean<IQueue<?>> {

	@SuppressWarnings("unchecked")
	protected ItemListener listener;
	
	private StatisticsCollector receivedStats = null;
	private StatisticsCollector servedStats = null;
	
	public QueueMBean(IQueue<?> queue) {
    	super(queue);
    }

	@Override
	public ObjectNameSpec getNameSpec() {
		return getParentName().getNested("Queue", getName());
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
				}

				public void itemRemoved(Object item) {
					servedStats.addEvent();
				}
			};
			getManagedObject().addItemListener(listener, false);
			
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
	@JMXDescription("Registration name of the queue")
	public String getName() {
		return getManagedObject().getName();
	}
	
	@JMXAttribute("Size")
	@JMXDescription("Current queue size")
	public int size() {
		return getManagedObject().size();
	}
	
	@JMXAttribute("ItemsReceived")
	@JMXDescription("Total items pushed in the queue since creation")
	public long getItemsReceived() {
		return receivedStats.getTotal();
	}

	@JMXAttribute("ItemsReceivedLast")
	@JMXDescription("Items pushed in the queue in the last second")
	public double getItemsReceivedAvg() {
		return receivedStats.getAverage();
	}

	@JMXAttribute("ItemsReceivedPeak")
	@JMXDescription("Max items pushed in the queue per second")
	public double getItemsReceivedMax() {
		return receivedStats.getMax();
	}

	@JMXAttribute("ItemsServed")
	@JMXDescription("Total items pulled from the queue since creation")
	public long getItemsServed() {
		return servedStats.getTotal();
	}

	@JMXAttribute("ItemsServedLast")
	@JMXDescription("Items pulled from the queue in the last second")
	public double getItemsServedAvg() {
		return servedStats.getAverage();
	}

	@JMXAttribute("ItemsServedPeak")
	@JMXDescription("Max pulled from the queue per second")
	public double getItemsServedMax() {
		return servedStats.getMax();
	}

}
