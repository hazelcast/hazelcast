/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.hibernate.local;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.hibernate.RegionCache;
import com.hazelcast.util.Clock;

/**
 * A timestamp based local RegionCache
 */
public class TimestampsRegionCache extends LocalRegionCache implements RegionCache {

    public TimestampsRegionCache(final String name, final HazelcastInstance hazelcastInstance) {
        super(name, hazelcastInstance, null);
    }

    @Override
    public boolean put(Object key, Object value, Object currentVersion) {
        return update(key, value, currentVersion, null, null);
    }

    @Override
    protected MessageListener<Object> createMessageListener() {
        return new MessageListener<Object>() {
            public void onMessage(final Message<Object> message) {
                if (message.getPublishingMember().localMember()) {
                    return;
                }
                final Timestamp ts = (Timestamp) message.getMessageObject();
                final Object key = ts.getKey();

                for (;;) {
                    final Value value = cache.get(key);
                    final Long current = value != null ? (Long) value.getValue() : null;
                    if (current != null) {
                        if (ts.getTimestamp() > current) {
                            if (cache.replace(key, value, new Value(value.getVersion(),
                                    ts.getTimestamp(), Clock.currentTimeMillis()))) {
                                return;
                            }
                        } else {
                            return;
                        }
                    } else {
                        if (cache.putIfAbsent(key, new Value(null, ts.getTimestamp(),
                                Clock.currentTimeMillis())) == null) {
                            return;
                        }
                    }
                }
            }
        };
    }

    @Override
    protected Object createMessage(final Object key, final Object value, final Object currentVersion) {
        return new Timestamp(key, (Long) value);
    }

    final void cleanup() {
    }
}
