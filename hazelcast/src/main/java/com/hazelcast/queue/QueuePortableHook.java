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
import com.hazelcast.queue.client.*;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;

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
    public static final int ADD_LISTENER = 12;
    public static final int REMAINING_CAPACITY = 13;
    public static final int DESTROY = 14;

    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public PortableFactory createFactory() {

        ConstructorFunction<Integer, Portable> constructors[] = new ConstructorFunction[15];


        constructors[OFFER] = new ConstructorFunction<Integer, Portable>() {
            @Override
            public Portable createNew(Integer arg) {
                return new OfferRequest();
            }
        };
        constructors[SIZE] = new ConstructorFunction<Integer, Portable>() {
            @Override
            public Portable createNew(Integer arg) {
                return new SizeRequest();
            }
        };
        constructors[REMOVE] = new ConstructorFunction<Integer, Portable>() {
            @Override
            public Portable createNew(Integer arg) {
                return new RemoveRequest();
            }
        };
        constructors[POLL] = new ConstructorFunction<Integer, Portable>() {
            @Override
            public Portable createNew(Integer arg) {
                return new PollRequest();
            }
        };
        constructors[PEEK] = new ConstructorFunction<Integer, Portable>() {
            @Override
            public Portable createNew(Integer arg) {
                return new PeekRequest();
            }
        };
        constructors[ITERATOR] = new ConstructorFunction<Integer, Portable>() {
            @Override
            public Portable createNew(Integer arg) {
                return new IteratorRequest();
            }
        };
        constructors[DRAIN] = new ConstructorFunction<Integer, Portable>() {
            @Override
            public Portable createNew(Integer arg) {
                return new DrainRequest();
            }
        };
        constructors[CONTAINS] = new ConstructorFunction<Integer, Portable>() {
            @Override
            public Portable createNew(Integer arg) {
                return new ContainsRequest();
            }
        };
        constructors[COMPARE_AND_REMOVE] = new ConstructorFunction<Integer, Portable>() {
            @Override
            public Portable createNew(Integer arg) {
                return new CompareAndRemoveRequest();
            }
        };
        constructors[CLEAR] = new ConstructorFunction<Integer, Portable>() {
            @Override
            public Portable createNew(Integer arg) {
                return new ClearRequest();
            }
        };
        constructors[ADD_ALL] = new ConstructorFunction<Integer, Portable>() {
            @Override
            public Portable createNew(Integer arg) {
                return new AddAllRequest();
            }
        };
        constructors[ADD_LISTENER] = new ConstructorFunction<Integer, Portable>() {
            @Override
            public Portable createNew(Integer arg) {
                return new AddListenerRequest();
            }
        };
        constructors[REMAINING_CAPACITY] = new ConstructorFunction<Integer, Portable>() {
            @Override
            public Portable createNew(Integer arg) {
                return new RemainingCapacityRequest();
            }
        };
        constructors[DESTROY] = new ConstructorFunction<Integer, Portable>() {
            @Override
            public Portable createNew(Integer arg) {
                return new QueueDestroyRequest();
            }
        };

        return new ArrayPortableFactory(constructors);
    }

    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
