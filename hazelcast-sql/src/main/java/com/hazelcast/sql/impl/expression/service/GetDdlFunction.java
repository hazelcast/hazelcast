/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.expression.service;

import com.hazelcast.jet.impl.execution.CooperativeThread;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.TriExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.schema.Mapping;
import com.hazelcast.sql.impl.schema.SqlCatalogObject;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.security.AccessControlException;

import static com.hazelcast.jet.impl.JetServiceBackend.SQL_CATALOG_MAP_NAME;
import static com.hazelcast.sql.impl.expression.string.StringFunctionUtils.asVarchar;

public class GetDdlFunction extends TriExpression<String> {
    static final String RELATION_NAMESPACE = "relation";
    static final String DATACONNECTION_NAMESPACE = "dataconnection";

    public GetDdlFunction() {
    }

    public GetDdlFunction(Expression<?> operand1, Expression<?> operand2, Expression<?> operand3) {
        super(operand1, operand2, operand3);
    }

    @SuppressWarnings("checkstyle:NPathComplexity")
    @Override
    public String eval(Row row, ExpressionEvalContext context) {
        CooperativeThread.checkNonCooperative();

        String namespace = asVarchar(operand1, row, context);
        if (namespace == null) {
            throw QueryException.error("Namespace must not be null for GET_DDL");
        }

        if (namespace.equals(DATACONNECTION_NAMESPACE)) {
            NodeEngine nodeEngine = context.getNodeEngine();
            if (nodeEngine.getConfig().getSecurityConfig().isEnabled()) {
                throw new AccessControlException("GET_DDL is disabled for data connection in secure environment");
            }
        }

        String objectName = asVarchar(operand2, row, context);
        if (objectName == null) {
            throw QueryException.error("Object_name must not be null for GET_DDL");
        }

        // Ignore schema for now, the only supported schema at the moment is 'hazelcast.public'.

        IMap<?, ?> sqlCatalog = context.getNodeEngine().getHazelcastInstance().getMap(SQL_CATALOG_MAP_NAME);
        final String ddl;
        if (!(namespace.equals(RELATION_NAMESPACE) || namespace.equals(DATACONNECTION_NAMESPACE))) {
            throw QueryException.error(
                    "Namespace '" + namespace + "' is not supported. Only '" + RELATION_NAMESPACE + "' and '"
                            + DATACONNECTION_NAMESPACE + "' namespaces are supported.");
        }

        String keyName = objectName;
        if (namespace.equals(DATACONNECTION_NAMESPACE)) {
            keyName = QueryUtils.wrapDataConnectionKey(objectName);
        }

        final Object obj = sqlCatalog.get(keyName);
        if (obj == null) {
            throw QueryException.error("Object '" + objectName + "' does not exist in namespace '" + namespace + "'");
        } else if (obj instanceof SqlCatalogObject) {
            if (obj instanceof Mapping) {
                NodeEngine nodeEngine = context.getNodeEngine();
                if (nodeEngine.getConfig().getSecurityConfig().isEnabled()) {
                    throw new AccessControlException("GET_DDL is disabled for mapping in secure environment");
                }
            }
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
    public int getClassId() {
        return JetSqlSerializerHook.EXPRESSION_GET_DDL;
    }

    public static GetDdlFunction create(Expression<?> namespace, Expression<?> objectName, Expression<?> schema) {
        return new GetDdlFunction(namespace, objectName, schema);
    }
}
