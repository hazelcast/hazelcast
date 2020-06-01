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

import com.hazelcast.sql.impl.calcite.SqlToQueryType;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Base class for all tables in the Calcite integration:
 * <ul>
 *     <li>Maps field types defined in the {@code core} module to Calcite types</li>
 *     <li>Provides access to the underlying table and statistics</li>
 * </ul>
 */
public class HazelcastTable extends AbstractTable {

    private final Table target;
    private final Statistic statistic;

    private RelDataType rowType;
    private Set<String> hiddenFieldNames;

    public HazelcastTable(Table target, Statistic statistic) {
        this.target = target;
        this.statistic = statistic;
    }

    @SuppressWarnings("unchecked")
    public <T extends Table> T getTarget() {
        return (T) target;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (rowType != null) {
            return rowType;
        }

        hiddenFieldNames = new HashSet<>();

        List<RelDataTypeField> convertedFields = new ArrayList<>(target.getFieldCount());

        for (int i = 0; i < target.getFieldCount(); i++) {
            TableField field = target.getField(i);

            String fieldName = field.getName();
            QueryDataType fieldType = field.getType();
            QueryDataTypeFamily fieldTypeFamily = fieldType.getTypeFamily();

            SqlTypeName sqlTypeName = SqlToQueryType.map(fieldTypeFamily);

            if (sqlTypeName == null) {
                throw new IllegalStateException("Unexpected type family: " + fieldTypeFamily);
            }

            RelDataType relDataType = typeFactory.createSqlType(sqlTypeName);
            RelDataType nullableRelDataType = typeFactory.createTypeWithNullability(relDataType, true);

            RelDataTypeField convertedField = new RelDataTypeFieldImpl(fieldName, convertedFields.size(), nullableRelDataType);
            convertedFields.add(convertedField);

            if (field.isHidden()) {
                hiddenFieldNames.add(fieldName);
            }
        }

        rowType = new RelRecordType(StructKind.PEEK_FIELDS, convertedFields, false);

        return rowType;
    }

    @Override
    public Statistic getStatistic() {
        return statistic;
    }

    public boolean isHidden(String fieldName) {
        assert hiddenFieldNames != null;

        return hiddenFieldNames.contains(fieldName);
    }
}
