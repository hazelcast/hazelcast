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

package com.hazelcast.queue;

import com.hazelcast.nio.serialization.*;
import com.hazelcast.util.ConstructorFunction;

/**
 * @mdogan 8/24/12
 */
public final class QueueDataSerializerHook implements DataSerializerHook {

    static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.QUEUE_DS_FACTORY, -11);

    static final int OFFER = 0;
    static final int POLL = 1;
    static final int PEEK = 2;

    static final int OFFER_BACKUP = 3;
    static final int POLL_BACKUP = 4;

    public int getFactoryId() {
        return F_ID;
    }

    public DataSerializableFactory createFactory() {

        ConstructorFunction<Integer, IdentifiedDataSerializable> constructors[] = new ConstructorFunction[5]; 
        
        constructors[OFFER] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new OfferOperation();
            }
        };
        
        constructors[OFFER_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new OfferBackupOperation();
            }
        };
        constructors[POLL] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PollOperation();
            }
        };
        constructors[POLL_BACKUP] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PollBackupOperation();
            }
        };
        constructors[PEEK] = new ConstructorFunction<Integer, IdentifiedDataSerializable>() {
            public IdentifiedDataSerializable createNew(Integer arg) {
                return new PeekOperation();
            }
        };
        return new ArrayDataSerializableFactory(constructors);
    }
}
