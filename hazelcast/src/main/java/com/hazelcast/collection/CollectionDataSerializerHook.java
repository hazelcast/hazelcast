package com.hazelcast.collection;

import com.hazelcast.collection.list.AddBackupOperation;
import com.hazelcast.collection.list.AddOperation;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.util.ConstructorFunction;

/**
 * @ali 8/30/13
 */
public class CollectionDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.COLLECTION_DS_FACTORY, -20);

    public static int increment = 1;
    public static final int ADD = increment++;
    public static final int ADD_BACKUP = increment++;

    public int getFactoryId() {
        return F_ID;
    }

    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable> constructors[] = new ConstructorFunction[increment];

        constructors[ADD] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new AddOperation();
            }
        };
        constructors[ADD_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new AddBackupOperation();
            }
        };
        return new ArrayDataSerializableFactory(constructors);
    }
}
