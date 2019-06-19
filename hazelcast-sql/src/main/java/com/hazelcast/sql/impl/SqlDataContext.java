/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.QueryProviderImpl;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.schema.SchemaPlus;

public class SqlDataContext implements DataContext {

    private final SqlContext context;

    private final SchemaPlus rootSchema;

    private final QueryProvider queryProvider = new QueryProviderImpl() {
        @Override
        public <T> Enumerator<T> executeQuery(Queryable<T> queryable) {
            return queryable.enumerator();
        }
    };

    public SqlDataContext(SqlContext context) {
        this.context = context;
        this.rootSchema = context.getRootSchema().plus();
    }

    @Override
    public SchemaPlus getRootSchema() {
        return rootSchema;
    }

    @Override
    public JavaTypeFactory getTypeFactory() {
        return context.getTypeFactory();
    }

    @Override
    public QueryProvider getQueryProvider() {
        return queryProvider;
    }

    @Override
    public Object get(String name) {
        return context.getInternalParameters().get(name);
    }

}
