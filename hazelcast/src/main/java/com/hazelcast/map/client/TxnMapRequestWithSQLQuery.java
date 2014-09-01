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
package com.hazelcast.map.client;

import com.hazelcast.map.MapPortableHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.predicate.SqlPredicate;
import java.io.IOException;

/**
 * User: sancar
 * Date: 9/18/13
 * Time: 2:22 PM
 */
public class TxnMapRequestWithSQLQuery extends AbstractTxnMapRequest {

    String predicate;

    public TxnMapRequestWithSQLQuery() {
    }

    public TxnMapRequestWithSQLQuery(String name, TxnMapRequestType requestType, String predicate) {
        super(name, requestType, null, null, null);
        this.predicate = predicate;
    }

    protected Predicate getPredicate() {
        return SqlPredicate.createPredicate(predicate);
    }

    public int getClassId() {
        return MapPortableHook.TXN_REQUEST_WITH_SQL_QUERY;
    }

    protected void writeDataInner(ObjectDataOutput out) throws IOException {
        if (predicate != null) {
            out.writeBoolean(true);
            out.writeUTF(predicate);
        } else {
            out.writeBoolean(false);
        }
    }

    protected void readDataInner(ObjectDataInput in) throws IOException {
        final boolean hasPredicate = in.readBoolean();
        if (hasPredicate) {
            predicate = in.readUTF();
        }
    }

}

