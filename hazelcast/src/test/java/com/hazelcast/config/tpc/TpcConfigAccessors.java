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

package com.hazelcast.config.tpc;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.tpc.TpcServerBootstrap;

import java.util.List;

import static com.hazelcast.test.Accessors.getNode;

public class TpcConfigAccessors {
    public static TpcServerBootstrap getTpcServerBootstrap(HazelcastInstance hz) {
        return getNode(hz).getNodeEngine().getTpcServerBootstrap();
    }

    public static int getEventloopCount(HazelcastInstance hz) {
        return getTpcServerBootstrap(hz).getTpcEngine().reactorCount();
    }

    public static boolean isTpcEnabled(HazelcastInstance hz) {
        return getTpcServerBootstrap(hz).isEnabled();
    }

    public static TpcSocketConfig getClientSocketConfig(HazelcastInstance hz) {
        return getTpcServerBootstrap(hz).getClientSocketConfig();
    }

    public static List<Integer> getClientPorts(HazelcastInstance hz) {
        return getTpcServerBootstrap(hz).getClientPorts();
    }
}
