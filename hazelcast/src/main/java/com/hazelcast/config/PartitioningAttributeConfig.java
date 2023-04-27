/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.strategy.AttributePartitioningStrategy;

import java.io.IOException;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkHasText;

/**
 * Contains the configuration for attributes used to create
 * {@link AttributePartitioningStrategy}
 */
public class PartitioningAttributeConfig implements IdentifiedDataSerializable {
    private String attributeName;

    public PartitioningAttributeConfig() { }

    public PartitioningAttributeConfig(final PartitioningAttributeConfig config) {
        this.attributeName = config.attributeName;
    }

    public PartitioningAttributeConfig(final String attributeName) {
        this.attributeName = attributeName;
    }

    /**
     * Returns the name of the attribute.
     *
     * @return string with the name of the attribute
     */
    public String getAttributeName() {
        return attributeName;
    }

    /**
     * Sets the name of the attribute. Used internally only.
     *
     * @param attributeName - name of the attribute to set.
     */
    public void setAttributeName(final String attributeName) {
        this.attributeName = checkHasText(attributeName, "AttributeName must not be empty");
    }

    @Override
    public void writeData(final ObjectDataOutput out) throws IOException {
        out.writeString(attributeName);
    }

    @Override
    public void readData(final ObjectDataInput in) throws IOException {
        this.attributeName = in.readString();
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.PARTITION_ATTRIBUTE_CONFIG;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PartitioningAttributeConfig that = (PartitioningAttributeConfig) o;
        return Objects.equals(attributeName, that.attributeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributeName);
    }

    @Override
    public String toString() {
        return "PartitioningAttributeConfig{"
                + "attributeName='" + attributeName + '\''
                + '}';
    }
}
