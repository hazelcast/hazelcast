package com.hazelcast.collection;

import com.hazelcast.collection.list.*;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.util.ConstructorFunction;

/**
 * @ali 8/30/13
 */
public class CollectionDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.COLLECTION_DS_FACTORY, -20);

    public static int increment = 1;
    public static final int LIST_ADD = increment++;
    public static final int LIST_ADD_BACKUP = increment++;
    public static final int LIST_GET = increment++;
    public static final int COLLECTION_REMOVE = increment++;
    public static final int COLLECTION_REMOVE_BACKUP = increment++;
    public static final int COLLECTION_SIZE = increment++;
    public static final int COLLECTION_CLEAR = increment++;
    public static final int COLLECTION_CLEAR_BACKUP = increment++;
    public static final int LIST_SET = increment++;
    public static final int LIST_SET_BACKUP = increment++;
    public static final int LIST_REMOVE = increment++;

    public int getFactoryId() {
        return F_ID;
    }

    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable> constructors[] = new ConstructorFunction[increment];

        constructors[LIST_ADD] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ListAddOperation();
            }
        };
        constructors[LIST_ADD_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ListAddBackupOperation();
            }
        };
        constructors[LIST_GET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ListGetOperation();
            }
        };
        constructors[COLLECTION_REMOVE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionRemoveOperation();
            }
        };
        constructors[COLLECTION_REMOVE_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionRemoveBackupOperation();
            }
        };
        constructors[COLLECTION_SIZE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionSizeOperation();
            }
        };
        constructors[COLLECTION_CLEAR] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionClearOperation();
            }
        };
        constructors[COLLECTION_CLEAR_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new CollectionClearBackupOperation();
            }
        };
        constructors[LIST_SET] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ListSetOperation();
            }
        };
        constructors[LIST_SET_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ListSetBackupOperation();
            }
        };
        constructors[LIST_REMOVE] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new ListRemoveOperation();
            }
        };



        return new ArrayDataSerializableFactory(constructors);
    }
}
