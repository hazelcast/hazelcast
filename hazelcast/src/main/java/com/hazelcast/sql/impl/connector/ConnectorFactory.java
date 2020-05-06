/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.connector;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ConnectorFactory {

    // TODO: replace it with connector class and dynamic instantiation ??? (QueryDataType needs to be public then?)
    private static final Map<String, Connector> CONNECTORS_BY_TYPE = prepareConnectors();

    private static Map<String, Connector> prepareConnectors() {
        Map<String, Connector> connectors = new HashMap<>();
        connectors.put("PARTITIONED", new PartitionedMapConnector());
        connectors.put("REPLICATED", new ReplicatedMapConnector());
        return connectors;
    }

    public static Connector from(String type) {
        return Objects.requireNonNull(CONNECTORS_BY_TYPE.get(type.toUpperCase()), "Unknown type - " + type);
    }
}
