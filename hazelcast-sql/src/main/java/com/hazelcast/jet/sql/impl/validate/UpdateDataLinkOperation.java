package com.hazelcast.jet.sql.impl.validate;

import com.hazelcast.datalink.impl.InternalDataLinkService;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.jet.sql.impl.schema.DataLinkStorage;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.sql.impl.schema.datalink.DataLinkCatalogEntry;

import java.io.IOException;

/**
 * An operation sent from the member handling the CREATE/DROP DATA LINK command
 * to all members (including to self) to update the datalink instance according
 * to the change of the data link definition in the SQL catalog.
 * <p>
 * The operation is sent in fire-and-forget manner, possible inconsistencies are
 * handled through a background checker.
 */
public class UpdateDataLinkOperation extends Operation implements IdentifiedDataSerializable {

    private String dataLinkName;

    public UpdateDataLinkOperation() { }

    public UpdateDataLinkOperation(String dataLinkName) {
        this.dataLinkName = dataLinkName;
    }

    @Override
    public void run() throws Exception {
        InternalDataLinkService dlService = getNodeEngine().getDataLinkService();
        DataLinkStorage storage = new DataLinkStorage(getNodeEngine());
        DataLinkCatalogEntry dataLinkCatalogEntry = storage.get(dataLinkName);
        if (dataLinkCatalogEntry != null) {
            dlService.replaceSqlDataLink(dataLinkCatalogEntry.getName(), dataLinkCatalogEntry.getType(), dataLinkCatalogEntry.getOptions());
        } else {
            dlService.removeDataLink(dataLinkName);
        }
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.UPDATE_DATA_LINK_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeString(dataLinkName);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        dataLinkName = in.readString();
    }
}
