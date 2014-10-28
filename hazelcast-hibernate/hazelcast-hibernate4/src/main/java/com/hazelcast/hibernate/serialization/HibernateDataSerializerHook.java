package com.hazelcast.hibernate.serialization;

import com.hazelcast.hibernate.distributed.LockEntryProcessor;
import com.hazelcast.hibernate.distributed.UnlockEntryProcessor;
import com.hazelcast.hibernate.distributed.UpdateEntryProcessor;
import com.hazelcast.hibernate.local.Invalidation;
import com.hazelcast.hibernate.local.Timestamp;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.DataSerializerHook;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

/**
 * An implementation of {@link DataSerializerHook} which constructs any of the
 * {@link com.hazelcast.nio.serialization.DataSerializable} objects for the
 * hibernate module
 */
public class HibernateDataSerializerHook implements DataSerializerHook {

    /**
     * The factory id for this class
     */
    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.HIBERNATE_DS_FACTORY, F_ID_OFFSET_HIBERNATE);
    /**
     * @see Value
     */
    public static final int VALUE = 0;
    /**
     * @see ExpiryMarker
     */
    public static final int EXPIRY_MARKER = 1;
    /**
     * @see LockEntryProcessor
     */
    public static final int LOCK = 2;
    /**
     * @see UnlockEntryProcessor
     */
    public static final int UNLOCK = 3;
    /**
     * @see UpdateEntryProcessor
     */
    public static final int UPDATE = 4;
    /**
     * @see Invalidation
     */
    public static final int INVALIDATION = 5;
    /**
     * @see Timestamp
     */
    public static final int TIMESTAMP = 6;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new Factory();
    }

    private static class Factory implements DataSerializableFactory {
        @Override
        public IdentifiedDataSerializable create(int typeId) {
            IdentifiedDataSerializable result;
            switch (typeId) {
                case VALUE:
                    result = new Value();
                    break;
                case EXPIRY_MARKER:
                    result = new ExpiryMarker();
                    break;
                case LOCK:
                    result = new LockEntryProcessor();
                    break;
                case UNLOCK:
                    result = new UnlockEntryProcessor();
                    break;
                case UPDATE:
                    result = new UpdateEntryProcessor();
                    break;
                case INVALIDATION:
                    result = new Invalidation();
                    break;
                case TIMESTAMP:
                    result = new Timestamp();
                    break;
                default:
                    result = null;
            }
            return result;
        }
    }

}
