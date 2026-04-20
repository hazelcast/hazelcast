/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.services.NodeAware;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class ReflectiveAttributeTestObject implements Serializable, HazelcastInstanceAware, NodeAware {
    public static Integer staticInt = 456;

    private transient Node node;
    private transient HazelcastInstance hazelcastInstance;
    private transient HazelcastInstanceProxy hazelcastInstanceProxy;

    private final String name;
    private final boolean allowTransientFieldSetting;
    private final ReflectiveAttributeTestObject childNode;

    private final Collection<ReflectiveAttributeTestObject> objectAsList;
    private final ReflectiveAttributeTestObject[] objectAsArray;

    private final HazelcastInstance[] hazelcastInstanceArray = new HazelcastInstance[1];
    private final HazelcastInstance[][][] hazelcastInstanceMultiDimArray = new HazelcastInstance[1][][];
    private final List<HazelcastInstance> hazelcastInstanceList = new ArrayList<>(1);

    public ReflectiveAttributeTestObject(String name) {
        this(name, null, null, null, true);
    }

    public ReflectiveAttributeTestObject(String name, boolean allowTransientFieldSetting) {
        this(name, null, null, null, allowTransientFieldSetting);
    }

    public ReflectiveAttributeTestObject(String name,
                                         ReflectiveAttributeTestObject childNode,
                                         Collection<ReflectiveAttributeTestObject> pathElementsList,
                                         ReflectiveAttributeTestObject[] pathElementsArray,
                                         boolean allowTransientFieldSetting) {
        this.name = name;
        this.childNode = childNode == null ? this : childNode;
        this.objectAsList = pathElementsList;
        this.objectAsArray = pathElementsArray;
        this.allowTransientFieldSetting = allowTransientFieldSetting;
    }

    public static ReflectiveAttributeTestObject withList(String name,
                                                         ReflectiveAttributeTestObject... elements) {
        return new ReflectiveAttributeTestObject(name, null, Arrays.asList(elements), null, true);
    }

    public static ReflectiveAttributeTestObject withArray(String name,
                                                          ReflectiveAttributeTestObject... elements) {
        return new ReflectiveAttributeTestObject(name, null, null, elements, true);
    }

    public String getName() {
        return name;
    }

    public static String name() {
        return "static";
    }

    public static Integer getStaticValue() {
        return 123;
    }

    public ReflectiveAttributeTestObject child() {
        return childNode;
    }

    public static ReflectiveAttributeTestObject staticChild() {
        return new ReflectiveAttributeTestObject("staticChild");
    }

    public Collection<ReflectiveAttributeTestObject> getObjectAsList() {
        return objectAsList;
    }

    public ReflectiveAttributeTestObject[] getObjectAsArray() {
        return objectAsArray;
    }

    public void doVoid() {
    }

    public String value() {
        return name;
    }

    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        if (allowTransientFieldSetting) {
            this.hazelcastInstance = hazelcastInstance;
            this.hazelcastInstanceArray[0] = hazelcastInstance;
            this.hazelcastInstanceList.add(hazelcastInstance);
            try {
                if (hazelcastInstance instanceof HazelcastInstanceImpl hzImpl) {
                    Constructor<HazelcastInstanceProxy> constructor = HazelcastInstanceProxy.class
                            .getDeclaredConstructor(HazelcastInstanceImpl.class);
                    constructor.setAccessible(true);
                    this.hazelcastInstanceProxy = constructor.newInstance(hzImpl);
                }
            } catch (ReflectiveOperationException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    @Override
    public void setNode(Node node) {
        if (allowTransientFieldSetting) {
            this.node = node;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ReflectiveAttributeTestObject)) {
            return false;
        }
        ReflectiveAttributeTestObject that = (ReflectiveAttributeTestObject) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
