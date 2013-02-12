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

import com.hazelcast.nio.serialization.PortableFactory;

import java.util.Collection;
import java.util.LinkedList;

public class SerializationConfig {

    private int portableVersion = 0;

    private String portableFactoryClass;

    private PortableFactory portableFactory;

    private GlobalSerializerConfig globalSerializer;

    private Collection<TypeSerializerConfig> typeSerializers;

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
        this.portableVersion = portableVersion;
        return this;
    }

    public String getPortableFactoryClass() {
        return portableFactoryClass;
    }

    public SerializationConfig setPortableFactoryClass(String portableFactoryClass) {
        this.portableFactoryClass = portableFactoryClass;
        return this;
    }

    public PortableFactory getPortableFactory() {
        return portableFactory;
    }

    public SerializationConfig setPortableFactory(PortableFactory portableFactory) {
        this.portableFactory = portableFactory;
        return this;
    }
}
