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

package com.hazelcast.nio.serialization;

/**
 * Hazelcast Internal Factory ID Repository
 */
public enum FactoryIdRepository {

    DEFAULT(0),// factory id 0 is reserved for Cluster objects (Data, Address, Member etc)...
    SPI(-1),
    PARTITION(-2),
    CLIENT(-3),
    MAP(-10),
    QUEUE(-11),
    MULTIMAP(-12),
    EXECUTOR(-13),
    CDL(-14),
    LOCK(-15),
    SEMAPHORE(-16),
    ATOMIC_LONG(-17),
    TOPIC(-18),
    CLIENT_TXN(-19),
    COLLECTION(-20),
    ATOMIC_REFERENCE(-21),
    MAP_REDUCE(-23),
    WEB(-1000);

    private String serializationProperty;
    private String portableProperty;
    private int defaultValue;

    private String SERIALIZATION_PROPERTY_PREFIX="hazelcast.serialization.ds.";
    private String PORTABLE_PROPERTY_PREFIX="hazelcast.serialization.portable.";

    FactoryIdRepository(int defaultValue) {
        this.defaultValue=defaultValue;
        serializationProperty=SERIALIZATION_PROPERTY_PREFIX+this.name().toLowerCase();
        portableProperty=PORTABLE_PROPERTY_PREFIX+this.name().toLowerCase();
    }

    public String getSerializationProperty() {
        return serializationProperty;
    }

    public String getPortableProperty() {
        return portableProperty;
    }

    public int getDefaultValue() {
        return defaultValue;
    }

    /**
     * returns DataSerializable Factory Id
     * @param module defined in FactoryIdRepository
     * @return factoryId
     */
    public static int getDSFactoryId(FactoryIdRepository module) {
        return getValue(module.getSerializationProperty(), module.getDefaultValue());
    }

    /**
     * returns Portable Factory Id
     * @param module defined in FactoryIdRepository
     * @return factoryId
     */
    public static int getPortableFactoryId(FactoryIdRepository module) {
        return getValue(module.getPortableProperty(), module.getDefaultValue());
    }

    private static int getValue(String prop, int defaultId) {
        final String value = System.getProperty(prop);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException ignored) {
            }
        }
        return defaultId;
    }

}
