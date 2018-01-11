/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.util.Preconditions.checkHasText;

/**
 * Contains the configuration for an index in a map. This class should be used in combination
 * with the {@link MapConfig}. The reason to create an map index is to speed up searches for
 * particular map entries.
 */
public class MapIndexConfig implements IdentifiedDataSerializable {

    private static final ILogger LOG = Logger.getLogger(MapIndexConfig.class);

    private String attribute;
    private boolean ordered;
    private transient MapIndexConfigReadOnly readOnly;

    /**
     * Creates a MapIndexConfig without an attribute and with ordered set to {@code false}.
     */
    public MapIndexConfig() {
    }

    /**
     * Creates a MapIndexConfig with the given attribute and ordered setting.
     *
     * @param attribute the attribute that is going to be indexed
     * @param ordered   {@code true} if the index is ordered, {@code false} otherwise
     * @see #setOrdered(boolean)
     * @see #setAttribute(String)
     */
    public MapIndexConfig(String attribute, boolean ordered) {
        setAttribute(attribute);
        setOrdered(ordered);
    }

    public MapIndexConfig(MapIndexConfig config) {
        attribute = config.getAttribute();
        ordered = config.isOrdered();
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    public MapIndexConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new MapIndexConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * Gets the attribute that is going to be indexed. If no attribute is set, {@code null} is returned.
     *
     * @return the attribute to be indexed
     * @see #setAttribute(String)
     */
    public String getAttribute() {
        return attribute;
    }

    /**
     * Sets the attribute that is going to be indexed.
     *
     * @param attribute the attribute that is going to be indexed
     * @return the updated MapIndexConfig
     * @throws IllegalArgumentException if attribute is null or an empty string
     */
    public MapIndexConfig setAttribute(String attribute) {
        this.attribute = validateIndexAttribute(attribute);
        return this;
    }

    /**
     * Checks if the index should be ordered.
     *
     * @return {@code true} if ordered, {@code false} otherwise
     * @see #setOrdered(boolean)
     */
    public boolean isOrdered() {
        return ordered;
    }

    /**
     * Configures the index to be ordered or not ordered. Some indices can be ordered, such as age.
     * Sometimes you want to look for all people with an age equal or greater than X.
     * In other cases an ordered index doesn't make sense, such as a phone number for a person.
     *
     * @param ordered if the index should be an ordered index
     * @return the updated MapIndexConfig
     */
    public MapIndexConfig setOrdered(boolean ordered) {
        this.ordered = ordered;
        return this;
    }

    @Override
    public String toString() {
        return "MapIndexConfig{attribute='" + attribute + "', ordered=" + ordered + '}';
    }

    /**
     * Validates index attribute content.
     *
     * @param attribute attribute to validate
     * @return the attribute for fluent assignment
     */
    public static String validateIndexAttribute(String attribute) {
        checkHasText(attribute, "Map index attribute must contain text");
        String keyPrefix = KEY_ATTRIBUTE_NAME.value();
        if (attribute.startsWith(keyPrefix) && attribute.length() > keyPrefix.length()) {
            if (attribute.charAt(keyPrefix.length()) != '#') {
                LOG.warning(KEY_ATTRIBUTE_NAME.value() + " used without a following '#' char in index attribute '"
                        + attribute + "'. Don't you want to index a key?");
            }
        }
        return attribute;
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.MAP_INDEX_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(attribute);
        out.writeBoolean(ordered);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        attribute = in.readUTF();
        ordered = in.readBoolean();
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MapIndexConfig)) {
            return false;
        }

        MapIndexConfig that = (MapIndexConfig) o;
        if (ordered != that.ordered) {
            return false;
        }
        return attribute != null ? attribute.equals(that.attribute) : that.attribute == null;
    }

    @Override
    public final int hashCode() {
        int result = attribute != null ? attribute.hashCode() : 0;
        result = 31 * result + (ordered ? 1 : 0);
        return result;
    }
}
