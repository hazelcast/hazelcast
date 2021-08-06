package com.hazelcast.sql.impl.calcite.validate.types;

import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.sql.type.SqlTypeName;

public class HazelcastJsonType extends RelDataTypeImpl {
    public static final HazelcastJsonType INSTANCE = new HazelcastJsonType();

    private HazelcastJsonType() {
        computeDigest();
    }

    @Override
    protected void generateTypeString(final StringBuilder sb, final boolean withDetail) {
        sb.append("JSON");
    }

    @Override
    public SqlTypeName getSqlTypeName() {
        // TODO: investigate alternatives?
        // spec for this method allows us to return NULL,
        // however a lot of code within Calcite assumes that its never NULL
        return SqlTypeName.OTHER;
    }
}
