/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.management.request;

/**
 * Constants to identify serialized requests.
 */
public final class ConsoleRequestConstants {

    static final int REQUEST_TYPE_CLUSTER_STATE = 1;
    static final int REQUEST_TYPE_GET_THREAD_DUMP = 2;
    static final int REQUEST_TYPE_EXECUTE_SCRIPT = 3;
    static final int REQUEST_TYPE_EVICT_LOCAL_MAP = 4;
    static final int REQUEST_TYPE_CONSOLE_COMMAND = 5;
    static final int REQUEST_TYPE_MAP_CONFIG = 6;
    static final int REQUEST_TYPE_MEMBER_CONFIG = 8;
    static final int REQUEST_TYPE_CLUSTER_PROPERTIES = 9;
    static final int REQUEST_TYPE_LOGS = 11;
    static final int REQUEST_TYPE_MAP_ENTRY = 12;
    static final int REQUEST_TYPE_MEMBER_SYSTEM_PROPERTIES = 13;
    static final int REQUEST_TYPE_RUN_GC = 15;
    static final int REQUEST_TYPE_LOG_VERSION_MISMATCH = 17;
    static final int REQUEST_TYPE_MEMBER_SHUTDOWN = 18;
    static final int REQUEST_TYPE_SYSTEM_WARNINGS = 20;

    private ConsoleRequestConstants() {
    }
}
