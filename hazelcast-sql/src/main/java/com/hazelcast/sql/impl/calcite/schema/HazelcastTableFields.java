/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.calcite.schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;

/**
 * Collector of table fields. Fields are added as they are processed by Calcite.
 */
public class HazelcastTableFields {
    /** Field names. */
    private final List<String> fieldNames = new ArrayList<>(2);

    /** Fields. */
    private final List<RelDataTypeField> fields = new ArrayList<>(2);

    /** Whether the new field was created by the last call to the {@code getField()} method. */
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
            if (Util.matches(caseSensitive, field.getName(), fieldName)) {
                return field;
            }
        }

        // TODO: Handle star.

        // Add the field dynamically.
        // TODO: Vladimir Ozerov:
        // TODO: Note that if we return "ANY", then nested field access will not work. But otherwise,
        // TODO: we have a problem with type inferences, e.g. try to do "HAVING sum(field) > 1" query and
        // TODO: see what happens. One potential solution is to override *ALL* operator definitions with our
        // TODO: own type inference strategy, but this is a *LOT* of work. Also note that "STRUCT" access
        // TODO: doesn't work well for GROUP BY! E.g. you will have an exception if you do
        // TODO: "SELECT t.__key.field, SUM(field2) FROM t GROUP BY t.__key.field", and this behavior is hard coded
        // TODO: into Calcite's logic!

        // TODO: In order to get nested field access working with aforementioned problems, uncomment the following
        // TODO: line, then set HazelcastTableRelDataType.getSqlTypeName() to "ANY" instead of "ROW", then
        // TODO: uncomment HazelcastTypeFactory.createSqlType()
        // RelDataType type = new HazelcastTableRelDataType(typeFactory, new HazelcastTableFields());

        RelDataType type = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true);

        RelDataTypeField field = new RelDataTypeFieldImpl(fieldName, fields.size(), type);

        fields.add(field);
        fieldNames.add(field.getName());

        created = true;

        return field;
    }

    /**
     * @return Whether the new field was created by the last call to the {@code getField()} method.
     */
    public boolean isFieldCreated() {
        boolean res = created;

        if (res) {
            created = false;
        }

        return res;
    }
}
