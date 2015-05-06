/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.transaction.client;

import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FactoryIdHelper;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableHook;

import java.util.Collection;

/**
 * Factory class for client transaction related classes
 */
public class ClientTxnPortableHook implements PortableHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(FactoryIdHelper.CLIENT_TXN_PORTABLE_FACTORY, -19);

    public static final int CREATE = 1;
    public static final int COMMIT = 2;
    public static final int ROLLBACK = 3;
    public static final int PREPARE_XA = 4;
    public static final int CREATE_XA = 5;
    public static final int COMMIT_XA = 6;
    public static final int ROLLBACK_XA = 7;
    public static final int FINALIZE_XA = 8;
    public static final int COLLECT_XA = 9;
    public static final int CLEAR_XA = 10;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    public PortableFactory createFactory() {
        final PortableFactory factory = new PortableFactory() {
            @Override
            public Portable create(int classId) {
                switch (classId) {
                    case CREATE:
                        return new CreateTransactionRequest();
                    case COMMIT:
                        return new CommitTransactionRequest();
                    case ROLLBACK:
                        return new RollbackTransactionRequest();
                    case PREPARE_XA:
                        return new PrepareXATransactionRequest();
                    case CREATE_XA:
                        return new CreateXATransactionRequest();
                    case COMMIT_XA:
                        return new CommitXATransactionRequest();
                    case ROLLBACK_XA:
                        return new RollbackXATransactionRequest();
                    case FINALIZE_XA:
                        return new FinalizeXATransactionRequest();
                    case COLLECT_XA:
                        return new CollectXATransactionsRequest();
                    case CLEAR_XA:
                        return new ClearRemoteTransactionRequest();
                    default:
                        return null;
                }
            }
        };
        return factory;
    }

    @Override
    public Collection<ClassDefinition> getBuiltinDefinitions() {
        return null;
    }
}
