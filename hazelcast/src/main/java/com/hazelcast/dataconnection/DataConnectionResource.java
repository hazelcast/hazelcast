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

package com.hazelcast.dataconnection;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Locale;

import static java.util.Objects.requireNonNull;

/**
 * DataConnection Resource is an object for which a mapping can be created.
 * <p>
 * For example, JDBC returns the list of tables and views, Kafka returns the list
 * of topics, and for a filesystem the list of files etc.
 *
 * @since 5.3
 */
public class DataConnectionResource {

    private final String type;
    private final String[] name;

    /**
     * Create a Resource with given type and name
     */
    public DataConnectionResource(@Nonnull String type, @Nonnull String name) {
        this.type = requireNonNull(type);
        this.name = new String[] {requireNonNull(name)};
    }

    /**
     * Create a Resource with given type and name
     */
    public DataConnectionResource(@Nonnull String type, @Nonnull String... name) {
        this.type = requireNonNull(type);
        this.name = requireNonNull(name);
    }

    /**
     * Type of the resource, e.g. TABLE for JDBC connector
     */
    @Nonnull
    public String type() {
        return type;
    }

    /**
     * Name of the resource, e.g. name of the JDBC table, including any namespace prefix such as schema.
     */
    @Nonnull
    public String[] name() {
        return name;
    }

    @Override
    public String toString() {
        return "Resource{"
                + "type='" + type + '\''
                + ", name='" + Arrays.toString(name) + '\''
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DataConnectionResource dataConnectionResource = (DataConnectionResource) o;

        if (!type.equalsIgnoreCase(dataConnectionResource.type)) {
            return false;
        }
        return Arrays.equals(name, dataConnectionResource.name);
    }

    @Override
    public int hashCode() {
        int result = type.toLowerCase(Locale.ROOT).hashCode();
        result = 31 * result + Arrays.hashCode(name);
        return result;
    }
}
