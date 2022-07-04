/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.jmx;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.ClusterProperty;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import static com.hazelcast.internal.jmx.MBeans.createHazelcastMBeanOrNull;
import static com.hazelcast.internal.jmx.MBeans.getObjectTypeOrNull;

/**
 * Service responsible for registering hazelcast management beans to the platform management bean server.
 */
public class ManagementService implements DistributedObjectListener {

    static final String DOMAIN = "com.hazelcast";
    private static final int INITIAL_CAPACITY = 5;
    private static final Pattern OBJECT_NAME_QUOTE_PATTERN = Pattern.compile("[:\",=*?]");

    final HazelcastInstanceImpl instance;
    private final boolean enabled;
    private final ILogger logger;
    private final UUID registrationId;
    private final InstanceMBean instanceMBean;

    public ManagementService(HazelcastInstanceImpl instance) {
        this.instance = instance;
        this.logger = instance.getLoggingService().getLogger(getClass());
        this.enabled = instance.node.getProperties().getBoolean(ClusterProperty.ENABLE_JMX);
        if (!enabled) {
            this.instanceMBean = null;
            this.registrationId = null;
            return;
        }

        logger.info("Hazelcast JMX agent enabled.");
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        InstanceMBean instanceMBean;
        try {
            instanceMBean = createInstanceMBean(instance);
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

    protected InstanceMBean createInstanceMBean(HazelcastInstanceImpl instance) {
        return new InstanceMBean(instance, this);
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

    public static void shutdownAll(List<HazelcastInstanceProxy> instances) {
        for (HazelcastInstanceProxy instance : instances) {
            shutdown(instance.getName());
        }
    }

    public static void shutdown(String instanceName) {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            Set<ObjectName> entries = mbs.queryNames(new ObjectName(
                    DOMAIN + ":instance=" + quote(instanceName) + ",*"), null);
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
        unregisterDistributedObject(event.getServiceName(), (String) event.getObjectName());
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

    private void unregisterDistributedObject(String serviceName, String objectName) {
        final String objectType = getObjectTypeOrNull(serviceName);
        if (objectType == null) {
            return;
        }

        ObjectName beanName = createObjectName(objectType, objectName);
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        if (mbs.isRegistered(beanName)) {
            try {
                mbs.unregisterMBean(beanName);
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
            throw new IllegalArgumentException(e);
        }
    }

    public static String quote(String text) {
        return OBJECT_NAME_QUOTE_PATTERN.matcher(text).find() || text.indexOf('\n') >= 0
                ? ObjectName.quote(text) : text;
    }
}
