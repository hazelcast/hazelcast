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

package com.hazelcast.config;

import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.PortableFactory;

import java.util.*;

public class SerializationConfig {

    private int portableVersion = 0;

    private Map<Integer, String> dataSerializableFactoryClasses;

    private Map<Integer, DataSerializableFactory> dataSerializableFactories;

    private Map<Integer, String> portableFactoryClasses;

    private Map<Integer, PortableFactory> portableFactories;

    private GlobalSerializerConfig globalSerializer;

    private Collection<TypeSerializerConfig> typeSerializers;

    private boolean checkClassDefErrors = true;

    private Set<ClassDefinition> classDefinitions;

    public SerializationConfig() {
        super();
    }

    public GlobalSerializerConfig getGlobalSerializer() {
        return globalSerializer;
    }

    public void setGlobalSerializer(GlobalSerializerConfig globalSerializer) {
        this.globalSerializer = globalSerializer;
    }

    public Collection<TypeSerializerConfig> getTypeSerializers() {
        if (typeSerializers == null) {
            typeSerializers = new LinkedList<TypeSerializerConfig>();
        }
        return typeSerializers;
    }

    public SerializationConfig addTypeSerializer(TypeSerializerConfig typeSerializerConfig) {
        getTypeSerializers().add(typeSerializerConfig);
        return this;
    }

    public SerializationConfig setTypeSerializers(Collection<TypeSerializerConfig> typeSerializers) {
        this.typeSerializers = typeSerializers;
        return this;
    }

    public int getPortableVersion() {
        return portableVersion;
    }

    public SerializationConfig setPortableVersion(int portableVersion) {
        if (portableVersion < 0) {
            throw new IllegalArgumentException("Portable version cannot be negative!");
        }
        this.portableVersion = portableVersion;
        return this;
    }

    public Map<Integer, String> getDataSerializableFactoryClasses() {
        if (dataSerializableFactoryClasses == null) {
            dataSerializableFactoryClasses = new HashMap<Integer, String>();
        }
        return dataSerializableFactoryClasses;
    }

    public SerializationConfig setDataSerializableFactoryClasses(Map<Integer, String> dataSerializableFactoryClasses) {
        this.dataSerializableFactoryClasses = dataSerializableFactoryClasses;
        return this;
    }

    public SerializationConfig addDataSerializableFactoryClass(int factoryId, String dataSerializableFactoryClass) {
        getDataSerializableFactoryClasses().put(factoryId, dataSerializableFactoryClass);
        return this;
    }

    public Map<Integer, DataSerializableFactory> getDataSerializableFactories() {
        if (dataSerializableFactories == null) {
            dataSerializableFactories = new HashMap<Integer, DataSerializableFactory>();
        }
        return dataSerializableFactories;
    }

    public SerializationConfig setDataSerializablePortableFactories(Map<Integer, DataSerializableFactory> dataSerializableFactories) {
        this.dataSerializableFactories = dataSerializableFactories;
        return this;
    }

    public SerializationConfig addDataSerializableFactory(int factoryId, DataSerializableFactory dataSerializableFactory) {
        getDataSerializableFactories().put(factoryId, dataSerializableFactory);
        return this;
    }

    public Map<Integer, String> getPortableFactoryClasses() {
        if (portableFactoryClasses == null) {
            portableFactoryClasses = new HashMap<Integer, String>();
        }
        return portableFactoryClasses;
    }

    public SerializationConfig setPortableFactoryClasses(Map<Integer, String> portableFactoryClasses) {
        this.portableFactoryClasses = portableFactoryClasses;
        return this;
    }

    public SerializationConfig addPortableFactoryClass(int factoryId, String portableFactoryClass) {
        getPortableFactoryClasses().put(factoryId, portableFactoryClass);
        return this;
    }

    public Map<Integer, PortableFactory> getPortableFactories() {
        if (portableFactories == null) {
            portableFactories = new HashMap<Integer, PortableFactory>();
        }
        return portableFactories;
    }

    public SerializationConfig setPortableFactories(Map<Integer, PortableFactory> portableFactories) {
        this.portableFactories = portableFactories;
        return this;
    }

    public SerializationConfig addPortableFactory(int factoryId, PortableFactory portableFactory) {
        getPortableFactories().put(factoryId, portableFactory);
        return this;
    }

    public Set<ClassDefinition> getClassDefinitions() {
        if (classDefinitions == null) {
            classDefinitions = new HashSet<ClassDefinition>();
        }
        return classDefinitions;
    }

    public SerializationConfig addClassDefinition(ClassDefinition classDefinition) {
        if(!getClassDefinitions().add(classDefinition)) {
            throw new IllegalArgumentException("ClassDefinition for class-id[" +classDefinition.getClassId()
                    + "] already exists!");
        }
        return this;
    }

    public SerializationConfig setClassDefinitions(Set<ClassDefinition> classDefinitions) {
        this.classDefinitions = classDefinitions;
        return this;
    }

    public boolean isCheckClassDefErrors() {
        return checkClassDefErrors;
    }

    public SerializationConfig setCheckClassDefErrors(boolean checkClassDefErrors) {
        this.checkClassDefErrors = checkClassDefErrors;
        return this;
    }
}
