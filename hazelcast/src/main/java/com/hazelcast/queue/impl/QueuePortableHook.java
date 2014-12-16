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

package com.hazelcast.queue.impl;

import com.hazelcast.nio.serialization.ArrayPortableFactory;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableHook;
import com.hazelcast.queue.impl.client.AddAllRequest;
import com.hazelcast.queue.impl.client.AddListenerRequest;
import com.hazelcast.queue.impl.client.ClearRequest;
import com.hazelcast.queue.impl.client.CompareAndRemoveRequest;
import com.hazelcast.queue.impl.client.ContainsRequest;
import com.hazelcast.queue.impl.client.DrainRequest;
import com.hazelcast.queue.impl.client.IsEmptyRequest;
import com.hazelcast.queue.impl.client.IteratorRequest;
import com.hazelcast.queue.impl.client.OfferRequest;
import com.hazelcast.queue.impl.client.PeekRequest;
import com.hazelcast.queue.impl.client.PollRequest;
import com.hazelcast.queue.impl.client.RemainingCapacityRequest;
import com.hazelcast.queue.impl.client.RemoveListenerRequest;
import com.hazelcast.queue.impl.client.RemoveRequest;
import com.hazelcast.queue.impl.client.SizeRequest;
import com.hazelcast.queue.impl.client.TxnOfferRequest;
import com.hazelcast.queue.impl.client.TxnPeekRequest;
import com.hazelcast.queue.impl.client.TxnPollRequest;
import com.hazelcast.queue.impl.client.TxnSizeRequest;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;

/**
 * Provides a Portable hook for the queue operations.
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
    public static final int TXN_OFFER = 14;
    public static final int TXN_POLL = 15;
    public static final int TXN_SIZE = 16;
    public static final int TXN_PEEK = 17;
    public static final int REMOVE_LISTENER = 18;
    public static final int IS_EMPTY = 19;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public PortableFactory createFactory() {

        ConstructorFunction<Integer, Portable>[] constructors = new ConstructorFunction[IS_EMPTY + 1];

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
        constructors[TXN_OFFER] = new ConstructorFunction<Integer, Portable>() {
            @Override
            public Portable createNew(Integer arg) {
                return new TxnOfferRequest();
            }
        };
        constructors[TXN_POLL] = new ConstructorFunction<Integer, Portable>() {
            @Override
            public Portable createNew(Integer arg) {
                return new TxnPollRequest();
            }
        };
        constructors[TXN_SIZE] = new ConstructorFunction<Integer, Portable>() {
            @Override
            public Portable createNew(Integer arg) {
                return new TxnSizeRequest();
            }
        };
        constructors[TXN_PEEK] = new ConstructorFunction<Integer, Portable>() {
            @Override
            public Portable createNew(Integer arg) {
                return new TxnPeekRequest();
            }
        };
        constructors[REMOVE_LISTENER] = new ConstructorFunction<Integer, Portable>() {
            @Override
            public Portable createNew(Integer arg) {
                return new RemoveListenerRequest();
            }
        };
        constructors[IS_EMPTY] = new ConstructorFunction<Integer, Portable>() {
            @Override
            public Portable createNew(Integer arg) {
                return new IsEmptyRequest();
            }
        };

        return new ArrayPortableFactory(constructors);
    }

    @Override
    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
