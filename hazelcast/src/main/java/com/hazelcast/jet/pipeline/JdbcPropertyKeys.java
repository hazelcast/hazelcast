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

import com.hazelcast.function.FunctionEx;

import java.util.Properties;

/**
 * This class defines property keys that can be passed to JDBC connector. In turn the JDBC connector
 * uses these properties to change the JDBC connection's behavior
 */
public final class JdbcPropertyKeys {

    /**
     * Property key to be passed to specify fetch size of the JDBC connection
     * For usage  example see {@link Sources#jdbc(String, String, Properties, FunctionEx)} method
     */
    public static final String FETCH_SIZE = "fetchSize";

    /**
     * Property key to be passed to specify auto commit mode of the JDBC connection
     * For usage example see {@link Sources#jdbc(String, String, Properties, FunctionEx)} method
     */
    public static final String AUTO_COMMIT = "autoCommit";

    private JdbcPropertyKeys() {
    }
}
