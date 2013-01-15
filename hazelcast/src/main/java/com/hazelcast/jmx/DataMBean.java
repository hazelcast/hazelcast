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

package com.hazelcast.jmx;

import com.hazelcast.core.*;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manager of data instances and collects general statistics.
 *
 * @author Marco Ferrante, DISI - University of Genova
 */
@JMXDescription("Cluster statistics")
public class DataMBean extends AbstractMBean<HazelcastInstance> implements DistributedObjectListener {

    private final static Logger logger = Logger.getLogger(DataMBean.class.getName());

    private StatisticsCollector creationStats = null;
    private StatisticsCollector destructionStats = null;

    /**
     * Return the instrumentation wrapper to a instance.
     * See http://java.sun.com/javase/technologies/core/mntr-mgmt/javamanagement/best-practices.jsp
     *
     * @param distributedObject
     * @return dynamic mbean for the hazelcast instance
     * @throws Exception
     */
    private AbstractMBean buildMBean(DistributedObject distributedObject) throws Exception {
        if (distributedObject instanceof ITopic) {
            return new TopicMBean((ITopic) distributedObject, managementService);
        }
        if (distributedObject instanceof IQueue) {
            return new QueueMBean((IQueue) distributedObject, managementService);
        }
        if (distributedObject instanceof IList) {
            return new ListMBean((IList) distributedObject, managementService);
        }
        if (distributedObject instanceof ISet) {
            return new SetMBean((ISet) distributedObject, managementService);
        }
        if (distributedObject instanceof MultiMap) {
            return new MultiMapMBean((MultiMap) distributedObject, managementService);
        }
        if (distributedObject instanceof IMap) {
            return new MapMBean((IMap) distributedObject, managementService);
        }
        if (distributedObject instanceof ILock) {
            return new LockMBean((ILock) distributedObject, managementService);
        }
        if (distributedObject instanceof AtomicNumber) {
            return new AtomicNumberMBean((AtomicNumber) distributedObject, managementService);
        }
        if (distributedObject instanceof ICountDownLatch) {
            return new CountDownLatchMBean((ICountDownLatch) distributedObject, managementService);
        }
        if (distributedObject instanceof ISemaphore) {
            return new SemaphoreMBean((ISemaphore) distributedObject, managementService);
        }
        return null;
    }

    protected DataMBean(ManagementService managementService) {
        super(managementService.getInstance(), managementService);
    }

    @Override
    public ObjectNameSpec getNameSpec() {
        return getParentName().getNested("Statistics");
    }

    public void postRegister(Boolean registrationDone) {
        if (registrationDone) {
            creationStats = ManagementService.newStatisticsCollector();
            destructionStats = ManagementService.newStatisticsCollector();
            getManagedObject().addDistributedObjectListener(this);
            for (final DistributedObject distributedObject : getManagedObject().getDistributedObjects()) {
                registerInstance(distributedObject);
            }
        }
    }

    public void preDeregister() throws Exception {
        getManagedObject().removeDistributedObjectListener(this);
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
    }

    public void distributedObjectCreated(DistributedObjectEvent event) {
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, "Received created notification {0} {1}",
                    new String[]{event.getServiceName(), event.getObjectId().toString()});
        }
        if (creationStats != null) {
            creationStats.addEvent();
        }
        registerInstance(event.getDistributedObject());
    }

    public void distributedObjectDestroyed(DistributedObjectEvent event) {
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, "Received destroyed notification " + event.getDistributedObject().toString());
        }
        if (destructionStats != null) {
            destructionStats.addEvent();
        }
        unregisterInstance(event.getDistributedObject());
    }

    public void registerInstance(Object instance) {
        try {
            AbstractMBean mbean = buildMBean((DistributedObject) instance);
            if (mbean == null) {
                logger.log(Level.FINE, "Unsupported instance type " + instance.getClass().getName());
            } else {
                mbean.setParentName(getParentName());
                ObjectName name = mbean.getObjectName();
                logger.log(Level.FINEST, "Register MBean {0}", name);
                MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                //noinspection SynchronizeOnThis
                synchronized (this) {
                    if (!mbs.isRegistered(name)) {
                        mbs.registerMBean(mbean, name);
                    }
                }
            }
        } catch (Exception e) {
            logger.log(Level.FINE, "Unable to register MBean", e);
        }
    }

    public void unregisterInstance(Object instance) {
        try {
            AbstractMBean mbean = buildMBean((DistributedObject) instance);
            if (mbean == null) {
                logger.log(Level.FINE, "Unsupported instance type " + instance.getClass().getName());
            } else {
                mbean.setParentName(getParentName());
                ObjectName name = mbean.getObjectName();
                logger.log(Level.FINEST, "Unregister MBean {0}", name);
                MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                //noinspection SynchronizeOnThis
                synchronized (this) {
                    if (mbs.isRegistered(name)) {
                        mbs.unregisterMBean(name);
                    }
                }
            }
        } catch (Exception e) {
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
        Collection<DistributedObject> distributedObjects = getManagedObject().getDistributedObjects();
        return distributedObjects.size();
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
