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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.sql.pojos.Person;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Handler;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

// TODO: check fetch size, looks like it's too low by default
// TODO: think about remote vs local execution
// TODO: move to public package?
public class JdbcDriver extends Driver {

    static {
        new JdbcDriver().register();
    }

    @Override
    protected String getConnectStringPrefix() {
        return "jdbc:hazelcast:";
    }

    @Override
    protected Function0<CalcitePrepare> createPrepareFactory() {
        return SqlPrepare::new;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        info.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        info.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        info.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());

        CalciteConnection connection = (CalciteConnection) super.connect(url, info);

        URI uri;
        try {
            uri = new URI(new URI(url).getRawSchemeSpecificPart());
        } catch (URISyntaxException e) {
            throw new SQLException(e);
        }

        // TODO custom client config support
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().addAddress(uri.getHost() + ":" + uri.getPort());
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);

        // TODO proper schema support
        SchemaImpl schema = new SchemaImpl();
        schema.addTable("persons",
                new SqlTable(connection.getTypeFactory().createStructType(Person.class), client.getMap("persons")));
        connection.getRootSchema().add("hazelcast", schema);
        connection.setSchema("hazelcast");
        connection.getProperties().put("hazelcast-client", client);

        return connection;
    }

    @Override
    protected Handler createHandler() {
        Handler handler = super.createHandler();
        return new Handler() {
            @Override
            public void onConnectionInit(AvaticaConnection connection) throws SQLException {
                handler.onConnectionInit(connection);
            }

            @Override
            public void onConnectionClose(AvaticaConnection connection) {
                handler.onConnectionClose(connection);
                CalciteConnection calciteConnection = (CalciteConnection) connection;
                HazelcastInstance client = (HazelcastInstance) calciteConnection.getProperties().get("hazelcast-client");
                client.shutdown();
            }

            @Override
            public void onStatementExecute(AvaticaStatement statement, ResultSink resultSink) {
                handler.onStatementExecute(statement, resultSink);
            }

            @Override
            public void onStatementClose(AvaticaStatement statement) {
                handler.onStatementClose(statement);
            }
        };
    }

    @Override
    protected DriverVersion createDriverVersion() {
        return new DriverVersion("Hazelcast JDBC Driver", "0.1", "Hazelcast", BuildInfoProvider.getBuildInfo().getVersion(),
                false, 0, 1, 0, 1);
    }

    private static class SchemaImpl extends AbstractSchema {

        private final Map<String, Table> tableMap = new HashMap<>();

        private final Map<String, Table> unmodifiableTableMap = Collections.unmodifiableMap(tableMap);

        public void addTable(String name, Table table) {
            tableMap.put(name, table);
        }

        @Override
        protected Map<String, Table> getTableMap() {
            return unmodifiableTableMap;
        }

    }

}
