package com.hazelcast.sql.impl.calcite;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.validate.SqlMoniker;

import java.util.Collections;
import java.util.List;

// TODO: RelDataTypeDrillImpl
public class HazelcastCalciteCatalogReader extends CalciteCatalogReader {

    public HazelcastCalciteCatalogReader(CalciteSchema rootSchema, RelDataTypeFactory typeFactory,
        CalciteConnectionConfig config) {
        super(rootSchema, Collections.emptyList(), typeFactory, config);
    }

    @Override
    public Prepare.PreparingTable getTable(List<String> names) {
        // TODO
        return super.getTable(names);
    }
}
