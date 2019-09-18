package com.hazelcast.sql.impl.calcite;

import com.hazelcast.spi.impl.NodeEngine;

import java.util.EnumMap;
import java.util.Map;

/**
 * Static context to keep local state during query planning.
 */
public class HazelcastCalciteContext {
    /** Thread-local context. */
    private static ThreadLocal<HazelcastCalciteContext> CTX = new ThreadLocal<>();

    /** Node engine. */
    private final NodeEngine nodeEngine;

    /** Cached data. */
    private final Map<Key, Object> data = new EnumMap<>(Key.class);

    /**
     * @return Current context.
     */
    public static HazelcastCalciteContext get() {
        return CTX.get();
    }

    /**
     * Initialize the context.
     *
     * @param nodeEngine Node engine.
     */
    public static void initialize(NodeEngine nodeEngine) {
        assert CTX.get() == null;

        CTX.set(new HazelcastCalciteContext(nodeEngine));
    }

    /**
     * Clear the context.
     */
    public static void clear() {
        CTX.remove();
    }

    /**
     * @return {@code True} if the given node stores data. Provided that the optimizer can only run on members,
     * lite member check is enough.
     */
    public boolean isDataMember() {
        return !nodeEngine.getLocalMember().isLiteMember();
    }

    @SuppressWarnings("unchecked")
    public <T> T getData(Key key) {
        return (T)data.get(key);
    }

    public void setData(Key key, Object value) {
        data.put(key, value);
    }

    private HazelcastCalciteContext(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public enum Key {
        PARTITIONED_MAPS,
        REPLICATED_MAPS
    }
}
