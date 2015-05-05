/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.logging.ILogger;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Hashtable;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import static com.hazelcast.jmx.MBeans.createHazelcastMBeanOrNull;
import static com.hazelcast.jmx.MBeans.getObjectTypeOrNull;

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
        HazelcastMBean bean = createHazelcastMBeanOrNull(distributedObject, this);
        if (bean == null) {
            return;
        }

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

    private void unregisterDistributedObject(DistributedObject distributedObject) {
        final String objectType = getObjectTypeOrNull(distributedObject);
        if (objectType == null) {
            return;
        }

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
