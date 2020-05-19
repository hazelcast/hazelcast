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

import java.util.Map;

public abstract class SqlKeyValueConnector implements SqlConnector {

    /**
     * A key in the table options (TO).
     * <p>
     * Specifies the key class in the entry. Can be omitted if "__key" is one
     * of the columns.
     */
    public static final String TO_KEY_CLASS = "keyClass";

    /**
     * A key in the table options (TO).
     * <p>
     * Specifies the value class in the entry. Can be omitted if "this" is one
     * of the columns.
     */
    public static final String TO_VALUE_CLASS = "valueClass";

    protected String keyClass(Map<String, String> options) {
        return options.get(TO_KEY_CLASS);
    }

    protected String valueClass(Map<String, String> options) {
        return options.get(TO_VALUE_CLASS);
    }
}
