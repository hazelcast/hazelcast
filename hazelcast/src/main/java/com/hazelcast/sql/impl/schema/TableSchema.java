package com.hazelcast.sql.impl.schema;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

/**
 * User defined table schema definition.
 */
public class TableSchema implements DataSerializable {

    private String name;
    private String type;
    private List<Entry<String, QueryDataType>> fields; // TODO: support for NULL/NON NULL, DEFAULT, WATERMARK... evolution
    private List<Entry<String, String>> options;

    @SuppressWarnings("unused")
    private TableSchema() {
    }

    public TableSchema(String name,
                       String type,
                       List<Entry<String, QueryDataType>> fields,
                       List<Entry<String, String>> options) {
        this.name = name;
        this.type = type;
        this.fields = fields;
        this.options = options;
    }

    public String name() {
        return name;
    }

    public String type() {
        return type;
    }

    public List<Entry<String, QueryDataType>> fields() {
        return fields;
    }

    public List<Entry<String, String>> options() {
        return options;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(type);
        out.writeObject(fields);
        out.writeObject(options);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        type = in.readUTF();
        fields = in.readObject();
        options = in.readObject();
    }
}
