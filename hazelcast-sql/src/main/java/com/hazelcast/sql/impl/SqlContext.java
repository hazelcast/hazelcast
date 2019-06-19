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
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.impl.LongSchemaVersion;
import org.apache.calcite.tools.RelRunner;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SqlContext implements CalcitePrepare.Context {

    private final JavaTypeFactory javaTypeFactory;

    private final CalciteSchema mutableRootSchema;

    private final CalciteSchema rootSchema;

    private final CalciteConnectionConfig connectionConfig;

    private final DataContext dataContext;

    private Map<String, Object> internalParameters;

    public SqlContext(JavaTypeFactory javaTypeFactory, CalciteSchema schema) {
        this.mutableRootSchema = schema;
        this.javaTypeFactory = javaTypeFactory;
        this.rootSchema = mutableRootSchema.createSnapshot(new LongSchemaVersion(System.nanoTime()));
        Properties properties = new Properties();
        properties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        properties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        properties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        this.connectionConfig = new CalciteConnectionConfigImpl(properties);
        this.dataContext = new SqlDataContext(this);
    }

    public void setInternalParameters(Map<String, Object> internalParameters) {
        this.internalParameters = internalParameters;
    }

    public Map<String, Object> getInternalParameters() {
        return internalParameters;
    }

    @Override
    public JavaTypeFactory getTypeFactory() {
        return javaTypeFactory;
    }

    @Override
    public CalciteSchema getRootSchema() {
        return rootSchema;
    }

    @Override
    public CalciteSchema getMutableRootSchema() {
        return mutableRootSchema;
    }

    @Override
    public List<String> getDefaultSchemaPath() {
        return Collections.emptyList();
    }

    @Override
    public CalciteConnectionConfig config() {
        return connectionConfig;
    }

    @Override
    public CalcitePrepare.SparkHandler spark() {
        final boolean enable = config().spark();
        return CalcitePrepare.Dummy.getSparkHandler(enable);
    }

    @Override
    public DataContext getDataContext() {
        return dataContext;
    }

    @Override
    public List<String> getObjectPath() {
        return null;
    }

    @Override
    public RelRunner getRelRunner() {
        throw new UnsupportedOperationException();
    }

}
