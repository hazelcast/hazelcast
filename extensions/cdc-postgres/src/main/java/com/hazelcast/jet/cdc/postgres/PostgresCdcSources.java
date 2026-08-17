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
package com.hazelcast.jet.cdc.postgres;

import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.DebeziumCdcSources;
import com.hazelcast.jet.cdc.RecordMappingFunction;
import com.hazelcast.jet.cdc.SequenceExtractor;
import com.hazelcast.jet.cdc.impl.ReadCdcP;
import com.hazelcast.jet.cdc.impl.ChangeRecordMappingFn;
import com.hazelcast.jet.cdc.impl.PropertyRules;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.cdc.DebeziumSnapshotMode;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.spi.snapshot.Snapshotter;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nonnull;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;

import static com.hazelcast.internal.nio.ClassLoaderUtil.newInstance;
import static com.hazelcast.internal.util.Preconditions.checkState;

/**
 * Contains factory methods for creating change data capture sources
 * based on PostgreSQL databases.
 *
 * @since Jet 4.2
 */
public final class PostgresCdcSources {

    private PostgresCdcSources() {
    }

    /**
     * Creates a CDC source that streams change data from a PostgreSQL database
     * to Hazelcast Jet.
     *
     * @param name name of this source, needs to be unique
     * @return builder that can be used to set source properties and also to
     * construct the source once configuration is done
     */
    @Nonnull
    public static Builder<ChangeRecord> postgres(@Nonnull String name) {
        return new Builder<>(name);
    }

    /**
     * Builder for configuring a CDC source that streams change data
     * from a PostgreSQL database to Hazelcast Jet.
     *
     * @param <T> type of items produced by the source
     */
    @SuppressWarnings("unused")
    public static final class Builder<T> extends DebeziumCdcSources.Builder<T> {

        private static final PropertyRules RULES = new PropertyRules()
                .required("database.hostname")
                .required("database.user")
                .required("database.password")
                .required("database.dbname")
                .inclusive("database.sslkey", "database.sslpassword")
                .exclusive("schema.include.list", "schema.exclude.list")
                .exclusive("table.include.list", "table.exclude.list");

        /**
         * @param name name of the source, needs to be unique
         */
        @SuppressWarnings("unchecked")
        private Builder(@Nonnull String name) {
            super(name, "io.debezium.connector.postgresql.PostgresConnector",
                    (RecordMappingFunction<T>) new ChangeRecordMappingFn());
            Objects.requireNonNull(name, "name cannot be null");
        }

        /**
         * Snapshot mode that will be used by the connector.
         * <p>
         * If you want to use {@link PostgresConnectorConfig.SnapshotMode#CUSTOM},
         * please use {@link #setCustomSnapshotter(Class)} method instead.
         */
        @Nonnull
        public Builder<T> setSnapshotMode(@Nonnull DebeziumSnapshotMode snapshotMode) {
            PostgresConnectorConfig.SnapshotMode debeziumMode = switch (snapshotMode) {
                case ALWAYS -> PostgresConnectorConfig.SnapshotMode.ALWAYS;
                case INITIAL -> PostgresConnectorConfig.SnapshotMode.INITIAL;
                case INITIAL_ONLY -> PostgresConnectorConfig.SnapshotMode.INITIAL_ONLY;
                case NO_DATA -> PostgresConnectorConfig.SnapshotMode.NO_DATA;
                case CONFIGURATION_BASED -> PostgresConnectorConfig.SnapshotMode.CONFIGURATION_BASED;
                case WHEN_NEEDED -> PostgresConnectorConfig.SnapshotMode.WHEN_NEEDED;
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
            config.setProperty("snapshot.mode", PostgresConnectorConfig.SnapshotMode.CUSTOM.getValue());
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

        /**
         * The name of the PostgreSQL database from which to stream the changes.
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
        //endregion

        //region Includes/Excludes
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
            config.setProperty("table.include.list", String.join(",", tableNameRegExps));
            return this;
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
            config.setProperty("table.exclude.list", String.join(",", tableNameRegExps));
            return this;
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
         * The name of the @see <a href="https://www.postgresql.org/docs/10/logicaldecoding.html">
         * Postgres logical decoding plug-in</a> installed on the server.
         * Supported values are <em>decoderbufs</em>, <em>wal2json</em>,
         * <em>wal2json_rds</em>, <em>wal2json_streaming</em>,
         * <em>wal2json_rds_streaming</em> and <em>pgoutput</em>.
         * <p>
         * If not explicitly set, the property defaults to <em>decoderbufs</em>.
         * <p>
         * When the processed transactions are very large it is possible that
         * the JSON batch event with all changes in the transaction will not fit
         * into the hard-coded memory buffer of size 1 GB. In such cases it is
         * possible to switch to so-called streaming mode when every change in
         * transactions is sent as a separate message from PostgreSQL.
         */
        @Nonnull
        public Builder<T> setLogicalDecodingPlugIn(@Nonnull String pluginName) {
            config.setProperty("plugin.name", pluginName);
            return this;
        }

        /**
         * The name of the @see <a href="https://www.postgresql.org/docs/10/logicaldecoding-explanation.html">
         * Postgres logical decoding slot</a> (also called "replication slot")
         * created for streaming changes from a plug-in and database instance.
         * <p>
         * Values must conform to Postgres replication slot naming rules which
         * state: "Each replication slot has a name, which can contain
         * lower-case letters, numbers, and the underscore character."
         * <p>
         * Replication slots have to have an identifier that is unique across
         * all databases in a PostgreSQL cluster.
         * <p>
         * If not explicitly set, the property defaults to <em>debezium</em>.
         */
        @Nonnull
        public Builder<T> setReplicationSlotName(@Nonnull String slotName) {
            config.setProperty("slot.name", slotName);
            return this;
        }

        /**
         * Whether to drop the logical replication slot when the
         * connector disconnects cleanly.
         * <p>
         * Defaults to <em>false</em>
         * <p>
         * Should only be set to <em>true</em> in testing or development
         * environments. Dropping the slot allows WAL segments to be discarded
         * by the database, so it may happen that after a restart the connector
         * cannot resume from the WAL position where it left off before.
         */
        @Nonnull
        public Builder<T> setReplicationSlotDropOnStop(boolean dropOnStop) {
            config.setProperty("slot.drop.on.stop", dropOnStop);
            return this;
        }

        /**
         * The name of the <a href="https://www.postgresql.org/docs/10/logical-replication-publication.html">
         * Postgres publication</a> that will be used for CDC purposes.
         * <p>
         * If the publication does not exist when this source starts up, then
         * the source will create it (note: the database user of the source must
         * have superuser permissions to be able to do so). If created this way
         * the publication will include all tables and the source itself must
         * filter the data based on its white-/blacklist configs. This is not
         * efficient because the database will still send all data to the
         * connector, before filtering is applied.
         * <p>
         * It's best to use a pre-defined publication (via the
         * <code>CREATE PUBLICATION</code> SQL command, specified via its name.
         * <p>
         * If not explicitly set, the property defaults to <em>dbz_publication</em>.
         */
        @Nonnull
        public Builder<T> setPublicationName(@Nonnull String publicationName) {
            config.setProperty("publication.name", publicationName);
            return this;
        }

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
         * Sets the return type of the source to {@link Entry} with key and value being {@link SourceRecord}'s
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
        public <T_NEW> Builder<T_NEW> customMapping(
                @Nonnull RecordMappingFunction<T_NEW> recordMappingFunction) {
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
        public PostgresCdcSources.Builder<T> setProperty(@Nonnull String key, int value) {
            return (Builder<T>) super.setProperty(key, value);
        }

        // endregion

        /**
         * Returns the source based on the properties set so far.
         */
        @Nonnull
        @Override
        public StreamSource<T> build() {
            config.setProperty(ReadCdcP.SEQUENCE_EXTRACTOR_CLASS_PROPERTY, PostgresSequenceExtractor.class.getName());
            Properties properties = config.toProperties();
            RULES.check(properties);
            return super.build();
        }

    }

}
