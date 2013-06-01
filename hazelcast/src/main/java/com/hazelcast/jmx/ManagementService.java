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

import com.hazelcast.core.*;
import com.hazelcast.instance.HazelcastInstanceImpl;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.Set;

/**
 * @ali 1/31/13
 */
public class ManagementService implements DistributedObjectListener {

    final HazelcastInstanceImpl instance;

    final boolean enabled;

    final boolean showDetails;

    private String registrationId;

    public ManagementService(HazelcastInstanceImpl instance) {
        this.instance = instance;
        this.enabled = instance.node.groupProperties.ENABLE_JMX.getBoolean();
        this.showDetails = instance.node.groupProperties.ENABLE_JMX_DETAILED.getBoolean();
        if (enabled) {
            init();
        }
    }

    public void init() {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            InstanceMBean instanceMBean = new InstanceMBean(instance, this);
            mbs.registerMBean(instanceMBean, instanceMBean.objectName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        registrationId = instance.addDistributedObjectListener(this);
        for (final DistributedObject distributedObject : instance.getDistributedObjects()) {
            registerDistributedObject(distributedObject);
        }
    }

    public void destroy() {
        if (!enabled) {
            return;
        }
        instance.removeDistributedObjectListener(registrationId);
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            Set<ObjectName> entries = mbs.queryNames(new ObjectName(
                    HazelcastMBean.DOMAIN + ":Cluster=" + instance.getName() + ",*"), null);
            for (ObjectName name : entries) {
                if (mbs.isRegistered(name)) {
                    mbs.unregisterMBean(name);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void distributedObjectCreated(DistributedObjectEvent event) {
        registerDistributedObject(event.getDistributedObject());
    }

    public void distributedObjectDestroyed(DistributedObjectEvent event) {
        unregisterDistributedObject(event.getDistributedObject());
    }

    private void registerDistributedObject(DistributedObject distributedObject) {
        HazelcastMBean bean = createHazelcastBean(distributedObject);
        if (bean != null){
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            if (!mbs.isRegistered(bean.objectName)) {
                try {
                    mbs.registerMBean(bean, bean.objectName);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void unregisterDistributedObject(DistributedObject distributedObject){
        HazelcastMBean bean = createHazelcastBean(distributedObject);
        if (bean != null){
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            if (mbs.isRegistered(bean.objectName)) {
                try {
                    mbs.unregisterMBean(bean.objectName);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void shutdownAll() {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            Set<ObjectName> entries = mbs.queryNames(new ObjectName(HazelcastMBean.DOMAIN + ":*"), null);
            for (ObjectName name : entries) {
                if (mbs.isRegistered(name)) {
                    mbs.unregisterMBean(name);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private HazelcastMBean createHazelcastBean(DistributedObject distributedObject){
        if (distributedObject instanceof IList){
            return new ListMBean((IList)distributedObject, this);
        }
        if (distributedObject instanceof IAtomicLong){
            return new AtomicLongMBean((IAtomicLong)distributedObject, this);
        }
        if (distributedObject instanceof ICountDownLatch){
            return new CountDownLatchMBean((ICountDownLatch)distributedObject, this);
        }
        if (distributedObject instanceof ILock){
            return new LockMBean((ILock)distributedObject, this);
        }
        if (distributedObject instanceof IMap){
            return new MapMBean((IMap)distributedObject, this);
        }
        if (distributedObject instanceof MultiMap){
            return new MultiMapMBean((MultiMap)distributedObject, this);
        }
        if (distributedObject instanceof IQueue){
            return new QueueMBean((IQueue)distributedObject, this);
        }
        if (distributedObject instanceof ISemaphore){
            return new SemaphoreMBean((ISemaphore)distributedObject, this);
        }
        if (distributedObject instanceof ISet){
            return new SetMBean((ISet)distributedObject, this);
        }
        if (distributedObject instanceof ITopic){
            return new TopicMBean((ITopic)distributedObject, this);
        }
        return null;
    }
}
