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

package com.hazelcast.sql;

/**
 * SQL exception throw in situation when query attempt might be retried immediately without any corrective actions.
 */
public class HazelcastSqlTransientException extends HazelcastSqlException {
    private static final long serialVersionUID = -2789247171606457719L;

    public HazelcastSqlTransientException(int code, String message) {
        super(code, message);
    }

    public HazelcastSqlTransientException(int code, String message, Throwable cause) {
        super(code, message, cause);
    }
}
