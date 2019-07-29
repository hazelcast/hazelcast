package com.hazelcast.sql.impl.calcite.schema;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypePrecedenceList;
import org.apache.calcite.sql.type.SqlTypeExplicitPrecedenceList;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

/**
 * Data type of a specific table.
 */
public class HazelcastTableRelDataType extends RelDataTypeImpl {

    private static final String TYPE_NAME = "HazelcastRow";

    private final RelDataTypeFactory typeFactory;
    private final HazelcastTableFields fields;

    public HazelcastTableRelDataType(RelDataTypeFactory typeFactory, HazelcastTableFields fields) {
        this.typeFactory = typeFactory;
        this.fields = fields;

        computeDigest();
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append("(").append(TYPE_NAME).append(getFieldNames()).append(")");
    }

    @Override
    public RelDataTypeField getField(String fieldName, boolean caseSensitive, boolean elideRecord) {
        RelDataTypeField res = fields.getField(typeFactory, fieldName, caseSensitive);

        if (fields.created())
            computeDigest();

        return res;
    }

    @Override
    public List<RelDataTypeField> getFieldList() {
        return fields.getFieldList();
    }

    @Override
    public List<String> getFieldNames() {
        return fields.getFieldNames();
    }

    @Override
    public int getFieldCount() {
        return fields.getFieldCount();
    }

    @Override
    public SqlTypeName getSqlTypeName() {
        return SqlTypeName.ROW;
    }

    @Override
    public RelDataTypePrecedenceList getPrecedenceList() {
        return new SqlTypeExplicitPrecedenceList(ImmutableList.of());
    }

    @Override
    public boolean isStruct() {
        return true;
    }

    @Override
    public boolean isDynamicStruct() {
        return true;
    }

    @Override
    public RelDataTypeFamily getFamily() {
        return getSqlTypeName().getFamily();
    }
}
