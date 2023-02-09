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

package com.hazelcast.jet.sql.impl.processors;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.sql.impl.parse.SqlCreateMapping;
import com.hazelcast.jet.sql.impl.parse.SqlCreateView;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.schema.Mapping;
import com.hazelcast.sql.impl.schema.view.View;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.impl.JetServiceBackend.SQL_CATALOG_MAP_NAME;

public class GetDdlP extends AbstractProcessor {
    static final String TABLE_NAMESPACE = "table";
    static final String DATALINK_NAMESPACE = "datalink";

    private transient IMap sqlCatalog;
    private transient InternalSerializationService ss;

    public GetDdlP() {
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        sqlCatalog = context.hazelcastInstance().getMap(SQL_CATALOG_MAP_NAME);
        ExpressionEvalContext evalContext = ExpressionEvalContext.from(context);
        ss = evalContext.getSerializationService();
        super.init(context);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        assert ordinal == 0;
        assert item instanceof JetSqlRow;

        JetSqlRow row = (JetSqlRow) item;
        String namespace = row.getRow().get(0);
        String objectName = row.getRow().get(1);

        final String ddl;
        if (!namespace.equals(TABLE_NAMESPACE)) {
            throw QueryException.error(
                    "Namespace '" + namespace + "' is not supported. Only 'table' namespace is supported.");
        }

        final Object obj = sqlCatalog.get(objectName);
        if (obj == null) {
            throw QueryException.error("Object '" + objectName + "' does not exist in namespace '" + namespace + "'");
        } else if (obj instanceof Mapping) {
            ddl = SqlCreateMapping.unparse((Mapping) obj);
        } else if (obj instanceof View) {
            ddl = SqlCreateView.unparse((View) obj);
        } else {
            throw new AssertionError("UNREACHABLE");
        }
        return tryEmit(new JetSqlRow(ss, new Object[]{ddl}));
    }
}