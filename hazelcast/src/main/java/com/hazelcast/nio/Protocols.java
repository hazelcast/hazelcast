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

package com.hazelcast.nio;

/**
 * First bytes to send a node that defines selected protocol
 */
public final class Protocols {

    /**
     * Protocol that is used among nodes
     */
    public static final String CLUSTER = "HZC";

    /**
     * Protocol that is used for clients(java, c++ , c# client)
     */
    public static final String CLIENT_BINARY = "CB1";

    /**
     * Protocol that is used by Memcache And Http
     */
    public static final String TEXT = "TXT";

    private Protocols() {
    }
}
