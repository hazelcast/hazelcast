package com.hazelcast.hibernate.local;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.hibernate.RegionCache;
import com.hazelcast.util.Clock;

/**
 * @mdogan 11/9/12
 */
public class TimestampsRegionCache extends LocalRegionCache implements RegionCache {

    public TimestampsRegionCache(final String name, final HazelcastInstance hazelcastInstance) {
        super(name, hazelcastInstance, null);
    }

    @Override
    protected MessageListener<Object> createMessageListener() {
        return new MessageListener<Object>() {
            public void onMessage(final Message<Object> message) {
                final Timestamp ts = (Timestamp) message.getMessageObject();
//                System.err.println("ts = " + ts);
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
