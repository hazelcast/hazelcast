/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.connection.AddressProvider;
import com.hazelcast.core.HazelcastInstance;

import static com.hazelcast.client.HazelcastClient.newHazelcastClientInternal;

public class HazelcastClientUtil {

    public static HazelcastInstance newHazelcastClient(AddressProvider addressProvider, ClientConfig clientConfig) {
        return newHazelcastClientInternal(addressProvider, clientConfig, null);
    }
}
