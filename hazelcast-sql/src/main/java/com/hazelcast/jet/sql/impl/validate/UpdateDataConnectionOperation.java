/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.validate;

import com.hazelcast.dataconnection.impl.InternalDataConnectionService;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.jet.sql.impl.schema.DataConnectionStorage;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.sql.impl.schema.dataconnection.DataConnectionCatalogEntry;

import java.io.IOException;

/**
 * An operation sent from the member handling the CREATE/DROP DATA CONNECTION command
 * to all members (including to self) to update the data connection instance according
 * to the change of the data connection definition in the SQL catalog.
 * <p>
 * The operation is sent in fire-and-forget manner, possible inconsistencies are
 * handled through a background checker.
 */
public class UpdateDataConnectionOperation extends Operation implements IdentifiedDataSerializable {

    private String dataConnectionName;

    public UpdateDataConnectionOperation() { }

    public UpdateDataConnectionOperation(String dataConnectionName) {
        this.dataConnectionName = dataConnectionName;
    }

    @Override
    public void run() throws Exception {
        InternalDataConnectionService dlService = getNodeEngine().getDataConnectionService();
        DataConnectionStorage storage = new DataConnectionStorage(getNodeEngine());
        DataConnectionCatalogEntry entry = storage.get(dataConnectionName);
        if (entry != null) {
            dlService.createOrReplaceSqlDataConnection(entry.name(), entry.type(), entry.isShared(), entry.options());
        } else {
            dlService.removeDataConnection(dataConnectionName);
        }
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.UPDATE_DATA_CONNECTION_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeString(dataConnectionName);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        dataConnectionName = in.readString();
    }
}
