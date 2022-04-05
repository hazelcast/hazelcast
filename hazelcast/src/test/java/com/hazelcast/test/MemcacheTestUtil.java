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

package com.hazelcast.test;

import javax.annotation.Nullable;

import com.hazelcast.logging.Logger;

import net.spy.memcached.MemcachedClient;

public final class MemcacheTestUtil {

    private MemcacheTestUtil() {
    }

    /**
     * Shutdowns {@link MemcachedClient} ignoring exceptions. Problems are just logged on FINE level. Workaround a
     * {@link java.util.ConcurrentModificationException} issue in Selector.shutdown() on IBM JDK.
     *
     * @param client a {@link MemcachedClient} to be shut down
     */
    public static void shutdownQuietly(@Nullable MemcachedClient client) {
        if (client == null) {
            return;
        }
        try {
            client.shutdown();
        } catch (Exception e) {
            Logger.getLogger(MemcacheTestUtil.class).fine("MemcachedClient.shutdown() failed. Ignoring.", e);
        }
    }
}
