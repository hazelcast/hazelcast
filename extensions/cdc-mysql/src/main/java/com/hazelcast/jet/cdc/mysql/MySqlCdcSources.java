/*
 * Copyright 2026 Hazelcast Inc.
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
package com.hazelcast.jet.cdc.mysql;

import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.DebeziumCdcSources;
import com.hazelcast.jet.cdc.RecordMappingFunction;
import com.hazelcast.jet.cdc.DebeziumSnapshotMode;
import com.hazelcast.jet.cdc.SequenceExtractor;
import com.hazelcast.jet.cdc.impl.ReadCdcP;
import com.hazelcast.jet.cdc.impl.ChangeRecordMappingFn;
import com.hazelcast.jet.cdc.impl.PropertyRules;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.pipeline.StreamSource;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.spi.snapshot.Snapshotter;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;

import static com.hazelcast.internal.nio.ClassLoaderUtil.newInstance;
import static com.hazelcast.internal.util.Preconditions.checkState;

/**
 * Contains factory methods for creating change data capture sources
 * based on MySQL databases.
 *
 * @since Jet 4.2
 */
public final class MySqlCdcSources {

    private MySqlCdcSources() {
    }

    /**
     * Creates a CDC source that streams change data from a MySQL database
     * to Hazelcast Jet.
     *
     * @param name name of this source, needs to be unique
     * @return builder that can be used to set source properties and also to
     * construct the source once configuration is done
     */
    @Nonnull
    public static Builder<ChangeRecord> mysql(@Nonnull String name) {
        return new Builder<>(name);
    }

    /**
     * Builder for configuring a CDC source that streams change data
     * from a MySQL database to Hazelcast Jet.
     *
     * @param <T> type of items produced by the source
     */
    @SuppressWarnings("unused")
    public static final class Builder<T> extends DebeziumCdcSources.Builder<T> {

        private static final PropertyRules RULES = new PropertyRules()
                .required("database.hostname")
                .required("database.user")
                .required("database.password")
                .required("database.server.id")
                .exclusive("database.dbname", "database.include.list")
                .exclusive("database.include.list", "database.exclusive.list")
                .inclusive("database.sslkey", "database.sslpassword")
                .exclusive("schema.include.list", "schema.exclude.list")
                .exclusive("table.include.list", "table.exclude.list");

        /**
         * @param name name of the source, needs to be unique
         */
        @SuppressWarnings("unchecked")
        private Builder(@Nonnull String name) {
            super(name, "io.debezium.connector.mysql.MySqlConnector",
                    (RecordMappingFunction<T>) new ChangeRecordMappingFn());
            Objects.requireNonNull(name, "name cannot be null");
            config.setProperty("database.server.id", String.valueOf(System.currentTimeMillis() % Integer.MAX_VALUE));
        }

        /**
         * Snapshot mode that will be used by the connector.
         * <p>
         * If you want to use {@link io.debezium.connector.mysql.MySqlConnectorConfig.SnapshotMode#CUSTOM},
         * please use {@link #setCustomSnapshotter(Class)} method instead.
         */
        @Nonnull
        public Builder<T> setSnapshotMode(@Nonnull DebeziumSnapshotMode snapshotMode) {
            MySqlConnectorConfig.SnapshotMode debeziumMode = switch (snapshotMode) {
                case ALWAYS -> MySqlConnectorConfig.SnapshotMode.ALWAYS;
                case INITIAL -> MySqlConnectorConfig.SnapshotMode.INITIAL;
                case INITIAL_ONLY -> MySqlConnectorConfig.SnapshotMode.INITIAL_ONLY;
                case NO_DATA -> MySqlConnectorConfig.SnapshotMode.NO_DATA;
                case CONFIGURATION_BASED -> MySqlConnectorConfig.SnapshotMode.CONFIGURATION_BASED;
                case WHEN_NEEDED -> MySqlConnectorConfig.SnapshotMode.WHEN_NEEDED;
            };
            config.setProperty("snapshot.mode", debeziumMode.getValue());
            return this;
        }

        /**
         * Custom snapshotter that will be used by the connector.
         */
        @Nonnull
        public Builder<T> setCustomSnapshotter(@Nonnull Class<?> snapshotterClass) {
            checkState(Snapshotter.class.isAssignableFrom(snapshotterClass), "snapshotterClass must be "
                    + "a subclass of Snapshotter");
            config.setProperty("snapshot.mode", MySqlConnectorConfig.SnapshotMode.CUSTOM.getValue());
            Snapshotter instance;
            try {
                instance = newInstance(getClass().getClassLoader(), snapshotterClass);
                config.setProperty("snapshot.custom.class", instance.name());
            } catch (Exception e) {
                throw new JetException("Cannot construct an instance of Snapshotter " + snapshotterClass, e);
            }
            return this;
        }

        /**
         * A numeric ID of this database client, which must be unique across all
         * currently-running database processes in the MySQL cluster. This
         * connector joins the MySQL database cluster as another server (with
         * this unique ID) so it can read the binlog. By default, a random
         * number is generated between 5400 and 6400, though we recommend
         * setting an explicit value.
         */
        @Nonnull
        public Builder<T> setDatabaseClientId(int clientId) {
            config.setProperty("database.server.id", Integer.toString(clientId));
            return this;
        }

        /**
         * IP address or hostname and the port of the database server, has to be specified.
         */
        @Nonnull
        public Builder<T> setDatabaseAddress(@Nonnull String address, int port) {
            config.setProperty("database.hostname", address);
            config.setProperty("database.port", Integer.toString(port));
            return this;
        }

        /**
         * Database user and password for connecting to the database server. Has to be
         * specified.
         */
        public Builder<T> setDatabaseCredentials(@Nonnull String user, @Nonnull String password) {
            config.setProperty("database.user", user);
            config.setProperty("database.password", password);
            return this;
        }

        // region Old functions for better compatibility
        /**
         * IP address or hostname of the database server, has to be specified.
         */
        @Nonnull
        public Builder<T> setDatabaseAddress(@Nonnull String address) {
            config.setProperty("database.hostname", address);
            return this;
        }

        /**
         * Optional port number of the database server, if unspecified defaults
         * to the database specific default port (5432).
         */
        @Nonnull
        public Builder<T> setDatabasePort(int port) {
            config.setProperty("database.port", Integer.toString(port));
            return this;
        }

        /**
         * Database user for connecting to the database server. Has to be
         * specified.
         */
        @Nonnull
        public Builder<T> setDatabaseUser(@Nonnull String user) {
            config.setProperty("database.user", user);
            return this;
        }

        /**
         * Database user password for connecting to the database server. Has to
         * be specified.
         */
        @Nonnull
        public Builder<T> setDatabasePassword(@Nonnull String password) {
            config.setProperty("database.password", password);
            return this;
        }
        //endregion

        //region Includes/Excludes
        /**
         * The name of the MySQL database from which to stream the changes.
         * Has to be set.
         * <p>
         * Currently, this source is not capable of monitoring multiple
         * databases, only multiple schemas and/or tables. See white- and
         * black-listing configuration options for those.
         */
        @Nonnull
        public Builder<T> setDatabaseName(@Nonnull String dbName) {
            config.setProperty("database.dbname", dbName);
            return this;
        }

        /**
         * Optional regular expressions that match schema names to be monitored
         * ("schema" is used here to denote logical groups of tables). Any
         * schema name not included in the whitelist will be excluded from
         * monitoring. By default, all non-system schemas will be monitored. May
         * not be used with
         * {@link #setSchemaExcludeList(String...) schema blacklist}.
         */
        @Nonnull
        public Builder<T> setSchemaIncludeList(@Nonnull String... schemaNameRegExps) {
            config.setProperty("schema.include.list", schemaNameRegExps);
            return this;
        }

        /**
         * Optional regular expressions that match schema names to be excluded
         * from monitoring ("schema" is used here to denote logical groups of
         * tables). Any schema name not included in the blacklist will be
         * monitored, except system schemas. May not be used with
         * {@link #setSchemaIncludeList(String...) schema whitelist}.
         */
        @Nonnull
        public Builder<T> setSchemaExcludeList(@Nonnull String... schemaNameRegExps) {
            config.setProperty("schema.exclude.list", schemaNameRegExps);
            return this;
        }

        /**
         * Optional regular expressions that match databases to be monitored; any database not included in the
         * include list will be excluded from monitoring. By default, the connector will
         * monitor all databases. May not be
         * used with {@link #setDatabaseExcludeList(String...) database exclude list}.
         */
        @Nonnull
        @Override
        public Builder<T> setDatabaseIncludeList(@Nonnull String... databaseNameRegExps) {
            return (Builder<T>) super.setDatabaseIncludeList(databaseNameRegExps);
        }

        /**
         * Optional regular expressions that match databases to be excluded from monitoring; any table not
         * included in the exclude list will be monitored. May not be used with
         * {@link #setDatabaseIncludeList(String...) database include list}.
         */
        @Nonnull
        @Override
        public Builder<T> setDatabaseExcludeList(@Nonnull String... databaseNameRegExps) {
            return (Builder<T>) super.setDatabaseExcludeList(databaseNameRegExps);
        }

        /**
         * Optional regular expressions that match fully-qualified table
         * identifiers for tables to be monitored; any table not included in the
         * whitelist will be excluded from monitoring. Each identifier is of the
         * form <em>schemaName.tableName</em>. By default, the connector will
         * monitor every non-system table in each monitored database. May not be
         * used with {@link #setTableExcludeList(String...) table exclude list}.
         */
        @Nonnull
        @Override
        public Builder<T> setTableIncludeList(@Nonnull String... tableNameRegExps) {
            return (Builder<T>) super.setTableIncludeList(tableNameRegExps);
        }

        /**
         * Optional regular expressions that match fully-qualified table
         * identifiers for tables to be excluded from monitoring; any table not
         * included in the blacklist will be monitored. Each identifier is of
         * the form <em>schemaName.tableName</em>. May not be used with
         * {@link #setTableIncludeList(String...) table whitelist}.
         */
        @Nonnull
        @Override
        public Builder<T> setTableExcludeList(@Nonnull String... tableNameRegExps) {
            return (Builder<T>) super.setTableExcludeList(tableNameRegExps);
        }

        /**
         * Optional regular expressions that match the fully-qualified names of
         * columns that should be excluded from change event message values.
         * Fully-qualified names for columns are of the form
         * <em>schemaName.tableName.columnName</em>.
         */
        @Nonnull
        public Builder<T> setColumnIncludeList(@Nonnull String... columnNameRegExps) {
            config.setProperty("column.include.list", columnNameRegExps);
            return this;
        }
        //endregion

        /**
         * Specifies whether to use an encrypted connection to the database. The
         * default is <em>disable</em>, and specifies to use an unencrypted
         * connection.
         * <p>
         * The <em>require</em> option establishes an encrypted connection but
         * will fail if one cannot be made for any reason.
         * <p>
         * The <em>verify_ca</em> option behaves like <em>require</em> but
         * additionally it verifies the server TLS certificate against the
         * configured Certificate Authority (CA) certificates and will fail if
         * it doesn’t match any valid CA certificates.
         * <p>
         * The <em>verify-full</em> option behaves like <em>verify_ca</em> but
         * additionally verifies that the server certificate matches the host
         * of the remote connection.
         */
        @Nonnull
        public Builder<T> setSslMode(@Nonnull String mode) {
            config.setProperty("database.sslmode", mode);
            return this;
        }

        /**
         * Specifies the (path to the) file containing the SSL Certificate for
         * the database client.
         */
        @Nonnull
        public Builder<T> setSslCertificateFile(@Nonnull String file) {
            config.setProperty("database.sslcert", file);
            return this;
        }

        /**
         * Specifies the (path to the) file containing the SSL private key of
         * the database client.
         */
        @Nonnull
        public Builder<T> setSslKeyFile(@Nonnull String file) {
            config.setProperty("database.sslkey", file);
            return this;
        }

        /**
         * Specifies the password to be used to access the SSL key file, if
         * specified.
         * <p>
         * Mandatory if key file specified.
         */
        @Nonnull
        public Builder<T> setSslKeyFilePassword(@Nonnull String password) {
            config.setProperty("database.sslpassword", password);
            return this;
        }

        /**
         * Specifies the file containing SSL certificate authority
         * (CA) certificate(s).
         */
        @Nonnull
        public Builder<T> setSslRootCertificateFile(@Nonnull String file) {
            config.setProperty("database.sslrootcert", file);
            return this;
        }

        // region Overrides
        /**
         * Sets the return type of the source to {@link ChangeRecord}.
         */
        @Nonnull
        @Override
        public Builder<ChangeRecord> changeRecord() {
            return (Builder<ChangeRecord>) super.changeRecord();
        }

        /**
         * Sets the return type of the source to {@link Map.Entry} with key and value being {@link SourceRecord}'s
         * key and value, parsed to json string.
         */
        @Nonnull
        @Override
        public Builder<Entry<String, String>> json() {
            return (Builder<Entry<String, String>>) super.json();
        }

        /**
         * Sets the return type of the source to user defined {@code #T_NEW} type. Mapping will be performed
         * by user-defined mapping function.
         */
        @Nonnull
        @Override
        public <T_NEW> Builder<T_NEW> customMapping(@Nonnull RecordMappingFunction<T_NEW> recordMappingFunction) {
            return (Builder<T_NEW>) super.customMapping(recordMappingFunction);
        }

        /**
         * Sets the {@link SequenceExtractor} class property.
         * <p>
         * Sequence extractor is used to determine monotonically increasing order of CDC order, that
         * can be used later in {@link com.hazelcast.jet.cdc.impl.WriteCdcP}.
         * @param sequenceExtractorClass Class of the SequenceExtractor implementation. Must be on the
         *                               classpath during execution.
         */
        @Nonnull
        @Override
        public Builder<T> withSequenceExtractor(Class<? extends SequenceExtractor> sequenceExtractorClass) {
            return (Builder<T>) super.withSequenceExtractor(sequenceExtractorClass);
        }

        /**
         * Sets the maximum retry count in case of errors.
         * <p>Default value is -1, which means infinite retries.
         */
        @Nonnull
        @Override
        public Builder<T> withErrorMaxRetries(int errorRetryCount) {
            return (Builder<T>) super.withErrorMaxRetries(errorRetryCount);
        }

        /**
         * Sets a source property. These properties are passed to Debezium.
         */
        @Nonnull
        @Override
        public Builder<T> setProperty(@Nonnull String key, @Nonnull String value) {
            return (Builder<T>) super.setProperty(key, value);
        }

        /**
         * Sets a source property. These properties are passed to Debezium.
         */
        @Nonnull
        @Override
        public Builder<T> setProperty(@Nonnull String key, long value) {
            return (Builder<T>) super.setProperty(key, value);
        }

        /**
         * Sets a source property. These properties are passed to Debezium.
         */
        @Nonnull
        @Override
        public Builder<T> setProperty(@Nonnull String key, boolean value) {
            return (Builder<T>) super.setProperty(key, value);
        }

        /**
         * Sets a source property. These properties are passed to Debezium.
         */
        @Nonnull
        @Override
        public Builder<T> setProperty(@Nonnull String key, int value) {
            return (Builder<T>) super.setProperty(key, value);
        }

        // endregion

        /**
         * Returns the source based on the properties set so far.
         */
        @Nonnull
        @Override
        public StreamSource<T> build() {
            config.setProperty(ReadCdcP.SEQUENCE_EXTRACTOR_CLASS_PROPERTY, MySqlSequenceExtractor.class.getName());
            Properties properties = config.toProperties();
            RULES.check(properties);
            return super.build();
        }

    }
}
