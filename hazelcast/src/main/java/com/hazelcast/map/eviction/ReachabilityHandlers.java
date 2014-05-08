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

package com.hazelcast.map.eviction;

import com.hazelcast.config.MapConfig;
import com.hazelcast.map.MapContainer;

import java.util.concurrent.TimeUnit;

/**
 * Static factory class.
 */
public final class ReachabilityHandlers {

    private ReachabilityHandlers() {
    }

    public static ReachabilityHandlerChain newHandlerChain(MapContainer container) {
        return newInstance(container);
    }

    private static ReachabilityHandlerChain newInstance(MapContainer container) {
        final MapConfig config = container.getMapConfig();
        final ReachabilityHandlerChain reachabilityHandlerChain = new ReachabilityHandlerChain();
        // zero max idle means eternal.
        if (config.getMaxIdleSeconds() != 0L) {
            reachabilityHandlerChain.addHandler(new IdleReachabilityHandler(TimeUnit.SECONDS.toNanos(config.getMaxIdleSeconds())));
        }
        // one can give ttl whn putting key&value pairs.
        reachabilityHandlerChain.addHandler(new TTLReachabilityHandler());
        return reachabilityHandlerChain;
    }

}
