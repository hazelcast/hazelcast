package com.hazelcast.topic;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;

/**
 * @mdogan 5/14/13
 */
public final class TopicDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.TOPIC_DS_FACTORY, -18);

    public int getFactoryId() {
        return F_ID;
    }

    public DataSerializableFactory createFactory() {
        return null;
    }
}
