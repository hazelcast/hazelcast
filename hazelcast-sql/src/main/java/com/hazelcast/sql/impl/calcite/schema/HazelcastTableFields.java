package com.hazelcast.sql.impl.calcite.schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;

public class HazelcastTableFields {
    private final List<String> fieldNames = new ArrayList<>(2);
    private final List<RelDataTypeField> fields = new ArrayList<>(2);
    private boolean created;

    public List<RelDataTypeField> getFieldList() {
        // TODO: Handle star.
        return fields;
    }

    public List<String> getFieldNames() {
        // TODO: Handle star.
        return fieldNames;
    }

    public int getFieldCount() {
        // TODO: Handle star.
        return fields.size();
    }

    public RelDataTypeField getField(RelDataTypeFactory typeFactory, String fieldName, boolean caseSensitive) {
        // Try get existing field.
        for (RelDataTypeField field : fields) {
            if (Util.matches(caseSensitive, field.getName(), fieldName))
                return field;
        }

        // TODO: Handle star.

        // Add the field dynamically.

        RelDataType type = new HazelcastTableRelDataType(typeFactory, new HazelcastTableFields());
        //RelDataType type = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true);

        RelDataTypeField field = new RelDataTypeFieldImpl(
            fieldName,
            fields.size(),
            type
        );

        fields.add(field);
        fieldNames.add(field.getName());

        created = true;

        return field;
    }

    public boolean created() {
        boolean res = created;

        if (res)
            created = false;

        return res;
    }
}
