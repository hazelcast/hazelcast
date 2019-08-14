package com.hazelcast.sql.impl.calcite;

import java.util.EnumMap;
import java.util.Map;

/**
 * Static context to keep local state during query planning.
 */
public class HazelcastCalciteContext {
    /** Thread-local context. */
    private static ThreadLocal<HazelcastCalciteContext> CTX = ThreadLocal.withInitial(HazelcastCalciteContext::new);

    /** Cached data. */
    private final Map<Key, Object> data = new EnumMap<>(Key.class);

    /**
     * @return Current context.
     */
    public static HazelcastCalciteContext get() {
        return CTX.get();
    }

    /**
     * Clear the context.
     */
    public static void clear() {
        CTX.remove();
    }

    @SuppressWarnings("unchecked")
    public <T> T getData(Key key) {
        return (T)data.get(key);
    }

    public void setData(Key key, Object value) {
        data.put(key, value);
    }

    private HazelcastCalciteContext() {
        // No-op.
    }

    public enum Key {
        PARTITIONED_MAPS,
        REPLICATED_MAPS
    }
}
