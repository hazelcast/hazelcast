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

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IList;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.replicatedmap.impl.ReplicatedMapProxy;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Hashtable;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import static com.hazelcast.util.EmptyStatement.ignore;

/**
 * Service responsible for registering hazelcast management beans to the platform management bean server.
 */
public class ManagementService implements DistributedObjectListener {

    static final String DOMAIN = "com.hazelcast";
    private static final int INITIAL_CAPACITY = 3;

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
        this.registrationId = instance.addDistributedObjectListener(this);
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
        if (bean != null) {
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

    private void unregisterDistributedObject(DistributedObject distributedObject) {
        final String objectType = getObjectType(distributedObject);
        if (objectType != null) {
            ObjectName objectName = createObjectName(objectType, distributedObject.getName());
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            if (mbs.isRegistered(objectName)) {
                try {
                    mbs.unregisterMBean(objectName);
                } catch (Exception e) {
                    logger.warning("Error while un-registering " + objectName, e);
                }
            }
        }
    }

    private HazelcastMBean createHazelcastBean(DistributedObject distributedObject) {
        try {
            HazelcastMBean bean = null;
            if (distributedObject instanceof IList) {
                bean = new ListMBean((IList) distributedObject, this);
            } else if (distributedObject instanceof IAtomicLong) {
                bean = new AtomicLongMBean((IAtomicLong) distributedObject, this);
            } else if (distributedObject instanceof IAtomicReference) {
                bean = new AtomicReferenceMBean((IAtomicReference) distributedObject, this);
            } else if (distributedObject instanceof ICountDownLatch) {
                bean = new CountDownLatchMBean((ICountDownLatch) distributedObject, this);
            } else if (distributedObject instanceof ILock) {
                bean = new LockMBean((ILock) distributedObject, this);
            } else if (distributedObject instanceof IMap) {
                bean = new MapMBean((IMap) distributedObject, this);
            } else if (distributedObject instanceof MultiMap) {
                bean = new MultiMapMBean((MultiMap) distributedObject, this);
            } else if (distributedObject instanceof IQueue) {
                bean = new QueueMBean((IQueue) distributedObject, this);
            } else if (distributedObject instanceof ISemaphore) {
                bean = new SemaphoreMBean((ISemaphore) distributedObject, this);
            } else if (distributedObject instanceof IExecutorService) {
                bean = new ExecutorServiceMBean((IExecutorService) distributedObject, this);
            } else if (distributedObject instanceof ISet) {
                bean = new SetMBean((ISet) distributedObject, this);
            } else if (distributedObject instanceof ITopic) {
                bean = new TopicMBean((ITopic) distributedObject, this);
            } else if (distributedObject instanceof ReplicatedMap) {
                bean = new ReplicatedMapMBean((ReplicatedMapProxy) distributedObject, this);
            }
            return bean;
        } catch (HazelcastInstanceNotActiveException ignored) {
            ignore(ignored);
        }
        return null;
    }

    private String getObjectType(DistributedObject distributedObject) {
        String result = null;
        if (distributedObject instanceof IList) {
            result = "IList";
        } else if (distributedObject instanceof IAtomicLong) {
            result = "IAtomicLong";
        } else if (distributedObject instanceof IAtomicReference) {
            result = "IAtomicReference";
        } else if (distributedObject instanceof ICountDownLatch) {
            result = "ICountDownLatch";
        } else if (distributedObject instanceof ILock) {
            result = "ILock";
        } else if (distributedObject instanceof IMap) {
            result = "IMap";
        } else if (distributedObject instanceof MultiMap) {
            result = "MultiMap";
        } else if (distributedObject instanceof IQueue) {
            result = "IQueue";
        } else if (distributedObject instanceof ISemaphore) {
            result = "ISemaphore";
        } else if (distributedObject instanceof ISet) {
            result = "ISet";
        } else if (distributedObject instanceof ITopic) {
            result = "ITopic";
        } else if (distributedObject instanceof IExecutorService) {
            result = "IExecutorService";
        } else if (distributedObject instanceof ReplicatedMap) {
            result = "ReplicatedMap";
        }
        return result;
    }

    protected ObjectName createObjectName(String type, String name) {
        Hashtable<String, String> properties = new Hashtable<String, String>(INITIAL_CAPACITY);
        properties.put("instance", quote(instance.getName()));
        if (type != null) {
            properties.put("type", quote(type));
        }
        if (name != null) {
            properties.put("name", quote(name));
        }
        try {
            return new ObjectName(DOMAIN, properties);
        } catch (MalformedObjectNameException e) {
            throw new IllegalArgumentException();
        }
    }

    public static String quote(String text) {
        return Pattern.compile("[:\",=*?]")
                .matcher(text)
                .find() ? ObjectName.quote(text) : text;
    }
}
