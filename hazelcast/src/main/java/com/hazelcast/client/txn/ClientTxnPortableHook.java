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

package com.hazelcast.client.txn;

import com.hazelcast.nio.serialization.*;

import java.util.Collection;

public class ClientTxnPortableHook implements PortableHook {

    public static final int F_ID = FactoryIdRepository.getPortableFactoryId(FactoryIdRepository.CLIENT_TXN);

    public static final int CREATE = 1;
    public static final int COMMIT = 2;
    public static final int ROLLBACK = 3;
    public static final int PREPARE = 4;
    public static final int RECOVER_ALL = 5;
    public static final int RECOVER = 6;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    public PortableFactory createFactory() {
        final PortableFactory factory = new PortableFactory() {
            public Portable create(int classId) {
                switch (classId) {
                    case CREATE:
                        return new CreateTransactionRequest();
                    case COMMIT:
                        return new CommitTransactionRequest();
                    case ROLLBACK:
                        return new RollbackTransactionRequest();
                    case PREPARE:
                        return new PrepareTransactionRequest();
                    case RECOVER_ALL:
                        return new RecoverAllTransactionsRequest();
                    case RECOVER:
                        return new RecoverTransactionRequest();
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
