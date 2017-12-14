/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Provides a basic configuration.
 *
 * @param <T> type of the config
 */
@SuppressWarnings("WeakerAccess")
public abstract class AbstractBasicConfig<T extends AbstractBasicConfig> implements IdentifiedDataSerializable {

    private String name;

    protected AbstractBasicConfig() {
    }

    protected AbstractBasicConfig(AbstractBasicConfig config) {
        this.name = config.name;
    }

    abstract T getAsReadOnly();

    /**
     * Gets the name of this collection.
     *
     * @return the name of this collection
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of this collection.
     *
     * @param name the name of this collection
     * @return the updated collection configuration
     */
    public T setName(String name) {
        this.name = checkNotNull(name, "name cannot be null");
        //noinspection unchecked
        return (T) this;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }

    @Override
    @SuppressWarnings("SimplifiableIfStatement")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractBasicConfig)) {
            return false;
        }

        AbstractBasicConfig<?> that = (AbstractBasicConfig<?>) o;
        return name != null ? name.equals(that.name) : that.name == null;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    /**
     * Returns field names with values as concatenated String so it can be used in child classes' toString() methods.
     */
    protected String fieldsToString() {
        return "name='" + name + "'";
    }
}
