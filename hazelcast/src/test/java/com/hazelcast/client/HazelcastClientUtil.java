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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.connection.AddressProvider;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;

public class HazelcastClientUtil {

    public static HazelcastInstance newHazelcastClient(AddressProvider addressProvider, ClientConfig clientConfig) {
        return HazelcastClient.newHazelcastClientInternal(addressProvider, clientConfig, null);
    }

    public static String getInstanceName(ClientConfig config) {
        return HazelcastClient.getInstanceName(config, null);
    }

    public static void registerProxyFuture(String instanceName,
                                           HazelcastInstanceFactory.InstanceFuture future) {
        HazelcastClient.getClients().put(instanceName, future);
    }

    public static boolean hasRegisteredClientWithName(String instanceName) {
        return HazelcastClient.getClients().containsKey(instanceName);
    }

    public static ClientMessage.Frame fastForwardToEndFrame(ClientMessage.ForwardFrameIterator iterator) {
        int numberOfExpectedEndFrames = 1;
        ClientMessage.Frame frame = null;
        while (numberOfExpectedEndFrames != 0) {
            frame = iterator.next();
            if (frame.isEndFrame()) {
                numberOfExpectedEndFrames--;
            } else if (frame.isBeginFrame()) {
                numberOfExpectedEndFrames++;
            }
        }
        return frame;
    }
}
