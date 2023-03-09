/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.expression.service;

import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.TriExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.schema.SqlCatalogObject;
import com.hazelcast.sql.impl.type.QueryDataType;

import static com.hazelcast.jet.impl.JetServiceBackend.SQL_CATALOG_MAP_NAME;
import static com.hazelcast.sql.impl.expression.string.StringFunctionUtils.asVarchar;

public class GetDdlFunction extends TriExpression<String> implements IdentifiedDataSerializable {
    static final String RELATION_NAMESPACE = "relation";
    static final String DATALINK_NAMESPACE = "datalink";

    public GetDdlFunction() {
    }

    public GetDdlFunction(Expression<?> operand1, Expression<?> operand2, Expression<?> operand3) {
        super(operand1, operand2, operand3);
    }

    @Override
    public String eval(Row row, ExpressionEvalContext context) {
        String namespace = asVarchar(operand1, row, context);
        if (namespace == null) {
            throw QueryException.error("Can't fetch DDL query for null namespace");
        }

        String objectName = asVarchar(operand2, row, context);
        if (objectName == null) {
            throw QueryException.error("Can't fetch DDL query for null object_name");
        }

        // Ignore schema for now, the only supported schema at the moment is 'hazelcast.public'.

        IMap sqlCatalog = context.getNodeEngine().getHazelcastInstance().getMap(SQL_CATALOG_MAP_NAME);
        final String ddl;
        if (!(namespace.equals(RELATION_NAMESPACE) || namespace.equals(DATALINK_NAMESPACE))) {
            throw QueryException.error(
                    "Namespace '" + namespace + "' is not supported."
                            + " Only '" + RELATION_NAMESPACE + "' and '" + DATALINK_NAMESPACE + "' namespaces are supported.");
        }

        String keyName = objectName;
        if (namespace.equals(DATALINK_NAMESPACE)) {
            keyName = QueryUtils.wrapDataLinkKey(objectName);
        }

        final Object obj = sqlCatalog.get(keyName);
        if (obj == null) {
            throw QueryException.error("Object '" + objectName + "' does not exist in namespace '" + namespace + "'");
        } else if (obj instanceof SqlCatalogObject) {
            ddl = ((SqlCatalogObject) obj).unparse();
        } else {
            throw new AssertionError("Object must not be present in information_schema");
        }
        return ddl;
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.VARCHAR;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_GET_DDL;
    }

    public static GetDdlFunction create(Expression<?> namespace, Expression<?> objectName, Expression<?> schema) {
        return new GetDdlFunction(namespace, objectName, schema);
    }
}
