package org.apache.calcite.jdbc;

import com.hazelcast.sql.impl.calcite.schema.HazelcastRootSchema;
import org.apache.calcite.schema.SchemaVersion;

public class HazelcastRootCalciteSchema extends SimpleCalciteSchema {

    public HazelcastRootCalciteSchema(HazelcastRootSchema schema) {
        super(null, schema, "");
    }

    @Override
    public CalciteSchema createSnapshot(SchemaVersion version) {
        return this;
    }
}
