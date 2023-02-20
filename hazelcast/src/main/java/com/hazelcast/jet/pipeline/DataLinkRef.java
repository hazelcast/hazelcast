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

package com.hazelcast.jet.pipeline;

import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.function.ToResultSetFunction;
import com.hazelcast.spi.annotation.Beta;

import java.io.Serializable;

/**
 * Represents a reference to the data link, used with
 * {@link Sources#jdbc(DataLinkRef, ToResultSetFunction, FunctionEx)}.
 *
 * @since 5.2
 */
@Beta
public class DataLinkRef implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String name;

    public DataLinkRef(String name) {
        this.name = name;
    }

    /**
     * Creates a reference to the configured data link
     *
     * @param name name of the data link configured in {@link DataLinkConfig}
     * @return the reference to the data link
     */
    public static DataLinkRef dataLinkRef(String name) {
        return new DataLinkRef(name);
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "DataLinkRef{" +
                "name='" + name + '\'' +
                '}';
    }
}
