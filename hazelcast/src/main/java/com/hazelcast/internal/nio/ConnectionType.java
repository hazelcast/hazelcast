/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nio;

import java.util.HashMap;
import java.util.Map;

/**
 * An enumeration of in-house Connection types.
 * Note that a type could be provided by a custom client
 * and it can be a string outside of {@link ConnectionType}
 */
@SuppressWarnings("MagicNumber")
public final class ConnectionType {

    /**
     * If connection type is not set yet
     */
    public static final String NONE = "NONE";

    /**
     * Member Connection Type
     */
    public static final String MEMBER = "MEMBER";

    /**
     * JVM clients Connection Type
     */
    public static final String JAVA_CLIENT = "JVM";

    /**
     * CSHARP client Connection Type
     */
    public static final String CSHARP_CLIENT = "CSP";

    /**
     * CPP client Connection Type
     */
    public static final String CPP_CLIENT = "CPP";

    /**
     * PYTHON client Connection Type
     */
    public static final String PYTHON_CLIENT = "PYH";

    /**
     * Node.JS client Connection Type
     */
    public static final String NODEJS_CLIENT = "NJS";

    /**
     * Go client Connection Type
     */
    public static final String GO_CLIENT = "GOO";

    /**
     * Rest client Connection Type
     */
    public static final String REST_CLIENT = "REST";

    /**
     * Memcache client Connection Type
     */
    public static final String MEMCACHE_CLIENT = "MEMCACHE";

    /**
     * Management Center Java client Connection Type
     */
    public static final String MC_JAVA_CLIENT = "MCJVM";

    /**
     * Command Line client Connection Type
     */
    public static final String CL_CLIENT = "CLC";

    private static final Map<String, Integer> ID_MAP = new HashMap<>();

    static {
        ID_MAP.put(NONE, 0);
        ID_MAP.put(MEMBER, 1);
        ID_MAP.put(JAVA_CLIENT, 2);
        ID_MAP.put(CPP_CLIENT, 3);
        ID_MAP.put(PYTHON_CLIENT, 4);
        ID_MAP.put(NODEJS_CLIENT, 5);
        ID_MAP.put(GO_CLIENT, 6);
        ID_MAP.put(REST_CLIENT, 7);
        ID_MAP.put(MEMCACHE_CLIENT, 8);
        ID_MAP.put(MC_JAVA_CLIENT, 9);
        ID_MAP.put(CSHARP_CLIENT, 10);
        ID_MAP.put(CL_CLIENT, 11);
    }

    private ConnectionType() {

    }

    /**
     * @param type name of the client
     * @return corresponding type id for in-house connection types(member/client),
     *         or -1 if connection is opened via a custom client
     */
    public static int getTypeId(String type) {
        return ID_MAP.getOrDefault(type, -1);
    }

}
