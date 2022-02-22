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

import com.hazelcast.spi.properties.ClusterProperty;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.IntrospectionException;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanRegistration;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

/**
 * Base class for beans registered to JMX by Hazelcast.
 *
 * @param <T> represents management bean to be registered.
 */
public abstract class HazelcastMBean<T> implements DynamicMBean, MBeanRegistration {

    protected HashMap<String, BeanInfo> attributeMap = new HashMap<String, BeanInfo>();
    protected HashMap<String, BeanInfo> operationMap = new HashMap<String, BeanInfo>();

    protected final long updateIntervalSec;

    protected final T managedObject;
    final ManagementService service;

    String description;
    ObjectName objectName;

    protected HazelcastMBean(T managedObject, ManagementService service) {
        this.managedObject = managedObject;
        this.service = service;
        this.updateIntervalSec = service.instance.node.getProperties().getLong(ClusterProperty.JMX_UPDATE_INTERVAL_SECONDS);
    }

    public static void register(HazelcastMBean mbean) {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            mbs.registerMBean(mbean, mbean.objectName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void unregister(HazelcastMBean mbean) {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        if (mbs.isRegistered(mbean.objectName)) {
            try {
                mbs.unregisterMBean(mbean.objectName);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void scan() throws Exception {
        ManagedDescription descAnn = getClass().getAnnotation(ManagedDescription.class);
        if (descAnn != null) {
            description = descAnn.value();
        }

        for (Method method : getClass().getMethods()) {

            if (method.isAnnotationPresent(ManagedAnnotation.class)) {
                ManagedAnnotation ann = method.getAnnotation(ManagedAnnotation.class);
                String name = ann.value();
                if (name.isEmpty()) {
                    throw new IllegalArgumentException("Name cannot be empty!");
                }
                boolean operation = ann.operation();
                HashMap<String, BeanInfo> map = operation ? operationMap : attributeMap;
                if (map.containsKey(name)) {
                    throw new IllegalArgumentException("Duplicate name: " + name);
                }
                descAnn = method.getAnnotation(ManagedDescription.class);
                String desc = null;
                if (descAnn != null) {
                    desc = descAnn.value();
                }
                map.put(name, new BeanInfo(name, desc, method));
            }
        }
    }

    @Override
    public Object getAttribute(String attribute)
            throws AttributeNotFoundException, MBeanException, ReflectionException {
        if (attribute == null || attribute.length() == 0) {
            throw new NullPointerException("Invalid null attribute requested");
        }
        BeanInfo info = attributeMap.get(attribute);
        try {
            return info.method.invoke(this);
        } catch (Exception e) {
            throw new ReflectionException(e);
        }
    }

    public void setObjectName(Map<String, String> properties) {
        try {
            objectName = new ObjectName(ManagementService.DOMAIN, new Hashtable<String, String>(properties));
        } catch (MalformedObjectNameException e) {
            throw new IllegalArgumentException("Failed to create an ObjectName", e);
        }
    }

    @Override
    public void setAttribute(Attribute attribute)
            throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public AttributeList getAttributes(String[] attributes) {
        AttributeList list = new AttributeList(attributes.length);
        try {
            for (String attribute : attributes) {
                list.add(new Attribute(attribute, getAttribute(attribute)));
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
        return list;
    }

    @Override
    public AttributeList setAttributes(AttributeList attributes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object invoke(String actionName, Object[] params, String[] signature)
            throws MBeanException, ReflectionException {
        if (actionName == null || actionName.isEmpty()) {
            throw new IllegalArgumentException("Empty actionName");
        }
        BeanInfo info = operationMap.get(actionName);
        if (info == null) {
            throw new UnsupportedOperationException("Operation: " + actionName + " not registered");
        }
        try {
            return info.method.invoke(this, params);
        } catch (Exception e) {
            throw new ReflectionException(e);
        }
    }

    @Override
    public MBeanInfo getMBeanInfo() {
        String className = managedObject.getClass().getName();
        return new MBeanInfo(className, description, attributeInfos(), null, operationInfos(), null);
    }

    private MBeanAttributeInfo[] attributeInfos() {
        MBeanAttributeInfo[] array = new MBeanAttributeInfo[attributeMap.size()];
        int i = 0;
        for (BeanInfo beanInfo : attributeMap.values()) {
            array[i++] = beanInfo.getAttributeInfo();
        }
        return array;
    }

    private MBeanOperationInfo[] operationInfos() {
        MBeanOperationInfo[] array = new MBeanOperationInfo[operationMap.size()];
        int i = 0;
        for (BeanInfo beanInfo : operationMap.values()) {
            array[i++] = beanInfo.getOperationInfo();
        }
        return array;
    }

    private class BeanInfo {

        final String name;
        final String description;
        transient Method method;

        BeanInfo(String name, String description, Method method) {
            this.name = name;
            this.description = description;
            this.method = method;
        }

        public MBeanAttributeInfo getAttributeInfo() {
            try {
                return new MBeanAttributeInfo(name, description, method, null);
            } catch (IntrospectionException e) {
                throw new IllegalArgumentException(e);
            }
        }

        public MBeanOperationInfo getOperationInfo() {
            return new MBeanOperationInfo(description, method);
        }
    }

    @Override
    public ObjectName preRegister(MBeanServer server, ObjectName name) throws Exception {
        try {
            scan();
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
        return objectName;
    }

    @Override
    public void postRegister(Boolean registrationDone) {
    }

    @Override
    public void preDeregister() throws Exception {
    }

    @Override
    public void postDeregister() {
    }
}
