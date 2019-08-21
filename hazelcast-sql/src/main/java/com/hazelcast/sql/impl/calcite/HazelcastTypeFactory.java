package com.hazelcast.sql.impl.calcite;

import com.hazelcast.sql.impl.calcite.schema.HazelcastTableFields;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTableRelDataType;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

public class HazelcastTypeFactory extends JavaTypeFactoryImpl {
    @Override
    public RelDataType createTypeWithNullability(RelDataType type, boolean nullable) {
        return super.createTypeWithNullability(type, nullable);
    }

    // TODO: Possible nested fields support. See big TODO in HazelcastTableFields.
//    @Override
//    public RelDataType createSqlType(SqlTypeName typeName) {
//        if (typeName == SqlTypeName.ANY)
//            return new HazelcastTableRelDataType(this, new HazelcastTableFields());
//
//        return super.createSqlType(typeName);
//    }
}
