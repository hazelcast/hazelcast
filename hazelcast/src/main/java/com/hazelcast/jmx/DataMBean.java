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

import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.management.DynamicMBean;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.Instance;
import com.hazelcast.core.InstanceEvent;
import com.hazelcast.core.InstanceListener;
import com.hazelcast.core.Member;
import com.hazelcast.impl.FactoryImpl;

/**
 * Manager of data instances and collects general statistics.
 * 
 * @author Marco Ferrante, DISI - University of Genova
 */
@JMXDescription("Data summary and statistics")
public class DataMBean extends AbstractMBean<Member> {

	private final static Logger logger = Logger.getLogger(DataMBean.class.getName());

	private InstanceListener instanceListener;
	
	private StatisticsCollector creationStats = null;
	private StatisticsCollector destructionStats = null;
	
    protected DataMBean() {
    	super(Hazelcast.getCluster().getLocalMember());
    }

	public ObjectName getObjectName() throws Exception {
		return MBeanBuilder.buildObjectName("type", "Data");
	}

	public void postRegister(Boolean registrationDone) {
		if (registrationDone) {

			creationStats = ManagementService.newStatisticsCollector();
			destructionStats = ManagementService.newStatisticsCollector();
			
			instanceListener = new InstanceListener() {

				public void instanceCreated(InstanceEvent event) {
					if (logger.isLoggable(Level.FINE)) {
						logger.log(Level.FINE, "Received created notification {0} {1}",
								new String[] {event.getInstance().getInstanceType().toString(), event.getInstance().toString()});
					}
					creationStats.addEvent();
					registerInstance(event.getInstance());
				}

				public void instanceDestroyed(InstanceEvent event) {
					if (logger.isLoggable(Level.FINE)) {
						logger.log(Level.FINE, "Received destroyed notification " + event.getInstance().toString());
					}
					destructionStats.addEvent();
					unregisterInstance(event.getInstance());
				}
				
			};
			Hazelcast.addInstanceListener(instanceListener);
		}
	}

	public void preDeregister() throws Exception {
		if (instanceListener != null) {
			Hazelcast.removeInstanceListener(instanceListener);
			instanceListener = null;
		}
		if (creationStats != null) {
			creationStats.destroy();
			creationStats = null;
		}
		if (destructionStats != null) {
			destructionStats.destroy();
			destructionStats = null;
		}
	}

	public void postDeregister() {
		// Required by MBeanRegistration interface 
		;
	}

	public void registerInstance(Object instance) {
		try {
			DynamicMBean mbean = MBeanBuilder.buildMBean((Instance)instance);
	    	
	    	if (mbean == null) {
	    		logger.log(Level.FINE, "Unsupported instance type "+  instance.getClass().getName());
	    	}
	    	else {
	            ObjectName name = MBeanBuilder.buildName(instance);
	    		logger.log(Level.FINEST, "Register MBean {0}", name);
	            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		    	synchronized(this) {
			    	if (!mbs.isRegistered(name)) {
			    		mbs.registerMBean(mbean, name);
			    	}
		    	}
	    	}
		}
		catch (Exception e) {
			logger.log(Level.FINE, "Unable to register MBean", e);
		} 
	}
	
	public void unregisterInstance(Object instance) {
		try {
            ObjectName name = MBeanBuilder.buildName(instance);
	    	
	    	if (name == null) {
	    		logger.log(Level.FINE, "Unsupported instance type "+  instance.getClass().getName());
	    	}
	    	else {
	    		logger.log(Level.FINEST, "Unregister MBean {0}", name);
	            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		    	synchronized(this) {
			    	if (mbs.isRegistered(name)) {
			    		mbs.unregisterMBean(name);
			    	}
		    	}
	    	}
		}
		catch (Exception e) {
			logger.log(Level.FINE, "Unable to unregister MBean", e);
		} 
	}
	
    /**
     * Resets statistics
     */
	@JMXOperation("resetStats")
    public void resetStats() {
    	if (creationStats != null)
    		creationStats.reset();
    	if (destructionStats != null)
    		destructionStats.reset();
    }
    
	@JMXAttribute("InstanceCount")
	@JMXDescription("Total data structures registered")
    public int getInstanceCount() {
    	Collection<Instance> instances = FactoryImpl.getFactory("default").getInstances();
    	
    	return instances.size();
    }

	@JMXAttribute("InstancesCreated")
	@JMXDescription("Total instances created since startup")
	public long getInstancesCreated() {
		return creationStats.getTotal();
	}

	@JMXAttribute("InstancesCreatedLast")
	@JMXDescription("Instances created in the last second")
	public double getInstancesCreatedAvg() {
		return creationStats.getAverage();
	}

	@JMXAttribute("InstancesCreatedPeak")
	@JMXDescription("Max instances created per second")
	public double getInstancesCreatedMax() {
		return creationStats.getMax();
	}

	@JMXAttribute("InstancesDestroyed")
	@JMXDescription("Total instances destroyed since startup")
	public long getInstancesDestroyed() {
		return destructionStats.getTotal();
	}

	@JMXAttribute("InstancesDestroyedLast")
	@JMXDescription("Instances destroyed in the last second")
	public double getInstancesDestroyedAvg() {
		return destructionStats.getAverage();
	}

	@JMXAttribute("InstancesDestroyedPeak")
	@JMXDescription("Max instances destroyed per second")
	public double getInstancesDestroyedMax() {
		return destructionStats.getMax();
	}
    
}
