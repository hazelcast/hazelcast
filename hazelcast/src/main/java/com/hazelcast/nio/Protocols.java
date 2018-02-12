/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio;

import com.hazelcast.spi.annotation.PrivateApi;

/**
 * First bytes to send a node that defines selected protocol
 */
@PrivateApi
public final class Protocols {

    /**
     * The length in bytes of the of the protocol-string
     */
    public static final int PROTOCOL_LENGTH = 3;

    /**
     * Protocol that is used among nodes
     */
    public static final String CLUSTER = "HZC";

    /**
     * New Client Protocol that is used for clients (Java, c++, c# client)
     */
    public static final String CLIENT_BINARY_NEW = "CB2";

    /**
     * Protocol that is used by Memcache And Http
     */
    public static final String TEXT = "TXT";

    private Protocols() {
    }

    public static String toUserFriendlyString(String protocol) {
        if (CLUSTER.equals(protocol)) {
            return "Cluster Protocol";
        }

        if (CLIENT_BINARY_NEW.equals(protocol)) {
            return "Client Open Binary Protocol";
        }

        if (TEXT.equals(protocol)) {
            return "Text Protocol";
        }

        return "Unknown Protocol";
    }
}
