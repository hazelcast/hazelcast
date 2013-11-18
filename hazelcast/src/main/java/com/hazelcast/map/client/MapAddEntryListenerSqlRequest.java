package com.hazelcast.map.client;

import com.hazelcast.map.MapPortableHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;

import java.io.IOException;

/**
 * User: sancar
 * Date: 11/11/13
 * Time: 11:03
 */
public class MapAddEntryListenerSqlRequest extends AbstractMapAddEntryListenerRequest {

    private String predicate;
    transient private Predicate cachedPredicate;

    public MapAddEntryListenerSqlRequest() {
        super();
    }

    public MapAddEntryListenerSqlRequest(String name, boolean includeValue) {
        super(name, includeValue);
    }

    public MapAddEntryListenerSqlRequest(String name, Data key, boolean includeValue) {
        super(name, key, includeValue);
    }

    public MapAddEntryListenerSqlRequest(String name, Data key, boolean includeValue, String predicate) {
        super(name, key, includeValue);
        this.predicate = predicate;
    }

    protected Predicate getPredicate() {
        if (cachedPredicate == null && predicate != null) {
            cachedPredicate = new SqlPredicate(predicate);
        }
        return cachedPredicate;
    }

    public int getClassId() {
        return MapPortableHook.ADD_ENTRY_LISTENER_SQL;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        final boolean hasKey = key != null;
        writer.writeBoolean("key", hasKey);
        if (predicate == null) {
            writer.writeBoolean("pre", false);
            if (hasKey) {
                final ObjectDataOutput out = writer.getRawDataOutput();
                key.writeData(out);
            }
        } else {
            writer.writeBoolean("pre", true);
            writer.writeUTF("p", predicate);
            final ObjectDataOutput out = writer.getRawDataOutput();
            if (hasKey) {
                key.writeData(out);
            }
        }

    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("name");
        includeValue = reader.readBoolean("i");
        boolean hasKey = reader.readBoolean("key");
        if (reader.readBoolean("pre")) {
            predicate = reader.readUTF("p");
            final ObjectDataInput in = reader.getRawDataInput();
            if (hasKey) {
                key = new Data();
                key.readData(in);
            }
        } else if (hasKey) {
            final ObjectDataInput in = reader.getRawDataInput();
            key = new Data();
            key.readData(in);
        }
    }

}
