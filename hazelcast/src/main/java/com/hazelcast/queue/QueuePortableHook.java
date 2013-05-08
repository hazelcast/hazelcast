/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableHook;
import com.hazelcast.queue.clientv2.*;

/**
 * @ali 5/8/13
 */
public class QueuePortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.QUEUE_PORTABLE_FACTORY, -11);

    public static final int OFFER = 1;
    public static final int SIZE = 2;
    public static final int REMOVE = 3;
    public static final int POLL = 4;
    public static final int PEEK = 5;
    public static final int ITERATOR = 6;
    public static final int DRAIN = 7;
    public static final int CONTAINS = 8;
    public static final int COMPARE_AND_REMOVE = 9;
    public static final int CLEAR = 10;
    public static final int ADD_ALL = 11;

    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public PortableFactory createFactory() {
        return new PortableFactory(){

            public Portable create(int classId) {
                switch (classId){
                    case OFFER:
                        return new OfferRequest();
                    case SIZE:
                        return new SizeRequest();
                    case REMOVE:
                        return new RemoveRequest();
                    case POLL:
                        return new PollRequest();
                    case PEEK:
                        return new PeekRequest();
                    case ITERATOR:
                        return new IteratorRequest();
                    case DRAIN:
                        return new DrainRequest();
                    case CONTAINS:
                        return new ContainsRequest();
                    case COMPARE_AND_REMOVE:
                        return new CompareAndRemoveRequest();
                    case CLEAR:
                        return new ClearRequest();
                    case ADD_ALL:
                        return new AddAllRequest();
                }
                return null;
            }
        };
    }
}
