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

package com.hazelcast.datalink;

import com.hazelcast.spi.annotation.Beta;

import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

/**
 * DataLink Resource is an object for which a mapping can be created.
 * <p>
 * For example, JDBC returns the list of tables and views, Kafka returns the list
 * of topics, and for a filesystem the list of files etc.
 *
 * @since 5.3
 */
@Beta
public class DataLinkResource {

    private final String type;
    private final String name;

    /**
     * Create a Resource with given type and name
     */
    public DataLinkResource(@Nonnull String type, @Nonnull String name) {
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
     * <p>
     * TODO Should we use String[]?
     */
    @Nonnull
    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return "Resource{"
                + "type='" + type + '\''
                + ", name='" + name + '\''
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

        DataLinkResource dataLinkResource = (DataLinkResource) o;

        if (!type.equals(dataLinkResource.type)) {
            return false;
        }
        return name.equals(dataLinkResource.name);
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }
}
