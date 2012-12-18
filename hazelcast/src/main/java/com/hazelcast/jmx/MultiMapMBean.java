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

import com.hazelcast.core.MultiMap;

/**
 * MBean for MultiMap
 *
 * @author Marco Ferrante, DISI - University of Genova
 */
@SuppressWarnings("unchecked")
@JMXDescription("A distributed MultiMap")
public class MultiMapMBean extends AbstractMBean<MultiMap> {

    public MultiMapMBean(MultiMap managedObject, ManagementService managementService) {
        super(managedObject, managementService);
    }

    @Override
    public ObjectNameSpec getNameSpec() {
        return getParentName().getNested("MultiMap", getName());
    }

    @JMXOperation("clear")
    @JMXDescription("Clear multi map")
    public void clear() {
        getManagedObject().clear();
    }

    @JMXAttribute("Name")
    @JMXDescription("Registration name of the list")
    public String getName() {
        return getManagedObject().getName();
    }

    @JMXAttribute("Size")
    @JMXDescription("Current size")
    public int getSize() {
        return getManagedObject().size();
    }
}
