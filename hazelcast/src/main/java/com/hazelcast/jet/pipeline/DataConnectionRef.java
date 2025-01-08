/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.function.ToResultSetFunction;

import java.io.Serial;
import java.io.Serializable;

import static java.util.Objects.requireNonNull;

/**
 * Represents a reference to the data connection, used with
 * {@link Sources#jdbc(DataConnectionRef, ToResultSetFunction, FunctionEx)}.
 *
 * @since 5.2
 */
public class DataConnectionRef implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String name;

    public DataConnectionRef(String name) {
        this.name = requireNonNull(name);
    }

    /**
     * Creates a reference to the configured data connection
     *
     * @param name name of the data connection configured in {@link DataConnectionConfig}
     * @return the reference to the data connection
     */
    public static DataConnectionRef dataConnectionRef(String name) {
        return new DataConnectionRef(name);
    }

    /**
     * If the name parameter is null, this method returns null.
     * Otherwise, it behaves identically to {@link #dataConnectionRef(String)}.
     */
    public static DataConnectionRef nullableDataConnectionRef(String name) {
        return name == null ? null : dataConnectionRef(name);
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "DataConnectionRef{" +
                "name='" + name + '\'' +
                '}';
    }
}
