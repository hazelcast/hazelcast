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
import com.hazelcast.logging.ILogger;
import com.hazelcast.replicatedmap.ReplicatedMapProxy;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Hashtable;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class ManagementService implements DistributedObjectListener {

    public static final String DOMAIN = "com.hazelcast";

    final HazelcastInstanceImpl instance;
    private final boolean enabled;
    private final ILogger logger;
    private final String registrationId;
    private final InstanceMBean instanceMBean;

    public ManagementService(HazelcastInstanceImpl instance) {
        this.instance = instance;
        this.logger = instance.getLoggingService().getLogger(getClass());
        this.enabled = instance.node.groupProperties.ENABLE_JMX.getBoolean();
        if (!enabled) {
            this.instanceMBean = null;
            this.registrationId = null;
            return;
        }

        logger.info("Hazelcast JMX agent enabled.");
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        InstanceMBean instanceMBean;
        try {
            instanceMBean = new InstanceMBean(instance, this);
            mbs.registerMBean(instanceMBean, instanceMBean.objectName);
        } catch (Exception e) {
            instanceMBean = null;
            logger.warning("Unable to start JMX service", e);

        }
        this.instanceMBean = instanceMBean;
        this.registrationId =  instance.addDistributedObjectListener(this);
        for (final DistributedObject distributedObject : instance.getDistributedObjects()) {
            registerDistributedObject(distributedObject);
        }
    }

    public InstanceMBean getInstanceMBean() {
        return instanceMBean;
    }

    public void destroy() {
        if (!enabled) {
            return;
        }
        instance.removeDistributedObjectListener(registrationId);
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            Set<ObjectName> entries = mbs.queryNames(new ObjectName(
                    DOMAIN + ":instance=" + quote(instance.getName()) + ",*"), null);
            for (ObjectName name : entries) {
                if (mbs.isRegistered(name)) {
                    mbs.unregisterMBean(name);
                }
            }
        } catch (Exception e) {
            logger.warning("Error while un-registering MBeans", e);
        }
    }

    public static void shutdownAll() {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            Set<ObjectName> entries = mbs.queryNames(new ObjectName(DOMAIN + ":*"), null);
            for (ObjectName name : entries) {
                if (mbs.isRegistered(name)) {
                    mbs.unregisterMBean(name);
                }
            }
        } catch (Exception e) {
            Logger.getLogger(ManagementService.class.getName())
                    .log(Level.WARNING, "Error while shutting down all jmx services...", e);
        }
    }

    @Override
    public void distributedObjectCreated(DistributedObjectEvent event) {
        registerDistributedObject(event.getDistributedObject());
    }

    @Override
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
                    logger.warning("Error while registering " + bean.objectName, e);
                }
            } else {
                try {
                    bean.preDeregister();
                    bean.postDeregister();
                } catch (Exception e) {
                    logger.finest(e);
                }
            }
        }
    }

    private void unregisterDistributedObject(DistributedObject distributedObject){
        final String objectType = getObjectType(distributedObject);
        if (objectType != null){
            ObjectName objectName = createObjectName(objectType, distributedObject.getName());
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            if (mbs.isRegistered(objectName)) {
                try {
                    mbs.unregisterMBean(objectName);
                } catch (Exception e) {
                    logger.warning( "Error while un-registering " + objectName, e);
                }
            }
        }
    }

    private HazelcastMBean createHazelcastBean(DistributedObject distributedObject){
        try {
            if (distributedObject instanceof IList){
                return new ListMBean((IList)distributedObject, this);
            }
            if (distributedObject instanceof IAtomicLong){
                return new AtomicLongMBean((IAtomicLong)distributedObject, this);
            }
            if (distributedObject instanceof IAtomicReference){
                return new AtomicReferenceMBean((IAtomicReference)distributedObject, this);
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
            if (distributedObject instanceof ReplicatedMapProxy){
                return new ReplicatedMapMBean((ReplicatedMapProxy)distributedObject, this);
            }
            if (distributedObject instanceof IQueue){
                return new QueueMBean((IQueue)distributedObject, this);
            }
            if (distributedObject instanceof ISemaphore){
                return new SemaphoreMBean((ISemaphore)distributedObject, this);
            }
            if (distributedObject instanceof IExecutorService){
                return new ExecutorServiceMBean((IExecutorService)distributedObject, this);
            }
            if (distributedObject instanceof ISet){
                return new SetMBean((ISet)distributedObject, this);
            }
            if (distributedObject instanceof ITopic){
                return new TopicMBean((ITopic)distributedObject, this);
            }
        } catch (HazelcastInstanceNotActiveException ignored) {
        }
        return null;
    }

    private String getObjectType(DistributedObject distributedObject){
        if (distributedObject instanceof IList){
            return "IList";
        }
        if (distributedObject instanceof IAtomicLong){
            return "IAtomicLong";
        }
        if (distributedObject instanceof IAtomicReference){
            return "IAtomicReference";
        }
        if (distributedObject instanceof ICountDownLatch){
            return "ICountDownLatch";
        }
        if (distributedObject instanceof ILock){
            return "ILock";
        }
        if (distributedObject instanceof IMap){
            return "IMap";
        }
        if (distributedObject instanceof MultiMap){
            return "MultiMap";
        }
        if (distributedObject instanceof IQueue){
            return "IQueue";
        }
        if (distributedObject instanceof ISemaphore){
            return "ISemaphore";
        }
        if (distributedObject instanceof ISet){
            return "ISet";
        }
        if (distributedObject instanceof ITopic){
            return "ITopic";
        }
        if (distributedObject instanceof IExecutorService){
            return "IExecutorService";
        }
        return null;
    }

    protected ObjectName createObjectName(String type, String name){
        Hashtable<String, String> properties = new Hashtable<String, String>(3);
        properties.put("instance", quote(instance.getName()));
        if (type != null){
            properties.put("type", quote(type));
        }
        if (name != null){
            properties.put("name", quote(name));
        }
        try {
            return new ObjectName(DOMAIN, properties);
        } catch (MalformedObjectNameException e) {
            throw new IllegalArgumentException();
        }
    }

    public static String quote(String text){
        return Pattern.compile("[:\",=*?]")
                .matcher(text)
                .find() ? ObjectName.quote(text) : text;
    }
}
