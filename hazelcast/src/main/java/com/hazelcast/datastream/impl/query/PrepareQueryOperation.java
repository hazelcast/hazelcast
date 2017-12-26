/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream.impl.query;

import com.hazelcast.datastream.impl.operations.DataStreamOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;

import java.io.IOException;

import static com.hazelcast.datastream.impl.DSDataSerializerHook.PREPARE_QUERY_OPERATION;

public class PrepareQueryOperation extends DataStreamOperation {

    public Predicate predicate;
    private String preparationId;

    public PrepareQueryOperation() {
    }

    public PrepareQueryOperation(String name,
                                 String preparationId,
                                 Predicate predicate) {
        super(name);
        this.predicate = predicate;
        this.preparationId = preparationId;
    }

    @Override
    public int getId() {
        return PREPARE_QUERY_OPERATION;
    }

    @Override
    public void run() throws Exception {
        partition.prepareQuery(preparationId, predicate);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(preparationId);
        out.writeObject(predicate);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        preparationId = in.readUTF();
        predicate = in.readObject();
    }
}
