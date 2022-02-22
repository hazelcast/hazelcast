/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.cdc.postgres;

import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.annotation.EvolvingApi;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.impl.CdcSourceP;
import com.hazelcast.jet.cdc.impl.ChangeRecordCdcSourceP;
import com.hazelcast.jet.cdc.impl.DebeziumConfig;
import com.hazelcast.jet.cdc.impl.PropertyRules;
import com.hazelcast.jet.cdc.postgres.impl.PostgresSequenceExtractor;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.retry.RetryStrategy;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Properties;

/**
 * Contains factory methods for creating change data capture sources
 * based on PostgreSQL databases.
 *
 * @since Jet 4.2
 */
@EvolvingApi
public final class PostgresCdcSources {

    private PostgresCdcSources() {
    }

    /**
     * Creates a CDC source that streams change data from a PostgreSQL database
     * to Hazelcast Jet.
     * <p>
     * You can configure how the source will behave if the database connection
     * breaks, by passing one of the {@linkplain RetryStrategy retry strategies}
     * to {@code setReconnectBehavior()}.
     * <p>
     * The default reconnect behavior is <em>never</em>, which treats any
     * connection failure as an unrecoverable problem and triggers the failure
     * of the source and the entire job.
     * <p>
     * Other behavior options, which specify that retry attempts should be
     * made, will result in the source initiating reconnects to the database.
     * <p>
     * There is a further setting influencing reconnect behavior, specified via
     * {@code setShouldStateBeResetOnReconnect()}. The boolean flag passed in
     * specifies what should happen to the connector's state on reconnect,
     * whether it should be kept or reset. If the state is kept, then
     * database snapshotting should not be repeated and streaming the WAL should
     * resume at the position where it left off. If the state is reset, then the
     * source will behave as on its initial start, so will do a database
     * snapshot and will start tailing the WAL where it syncs with the database
     * snapshot's end.
     * <p>
     * You can also configure how often the source will send feedback about
     * processed change record offsets to the backing database via
     * {@code setCommitPeriod()}. The replication slots of the database will
     * clean up their internal data structures based on this feedback. A commit
     * period of {@code 0} means that the source will commit offsets after every
     * batch of change records. Also, important to note that periodic commits
     * happen only in the case of jobs without processing guarantees. For jobs
     * offering processing guarantees, the source will ignore this setting and
     * commit offsets as part of the state snapshotting process. So the setting
     * governing them will be
     * {@linkplain JobConfig#setSnapshotIntervalMillis(long)
     * JobConfig.setSnapshotIntervalMillis}.
     *
     * @param name name of this source, needs to be unique, will be passed to
     *             the underlying Kafka Connect source
     * @return builder that can be used to set source properties and also to
     * construct the source once configuration is done
     */
    @Nonnull
    public static Builder postgres(@Nonnull String name) {
        return new Builder(name);
    }

    /**
     * Builder for configuring a CDC source that streams change data
     * from a PostgreSQL database to Hazelcast Jet.
     */
    public static final class Builder {

        private static final PropertyRules RULES = new PropertyRules()
                .required("database.hostname")
                .required("database.user")
                .required("database.password")
                .required("database.dbname")
                .inclusive("database.sslkey", "database.sslpassword")
                .exclusive("schema.whitelist", "schema.blacklist")
                .exclusive("table.whitelist", "table.blacklist");

        private final DebeziumConfig config;

        /**
         * @param name name of the source, needs to be unique, will be passed to
         *             the underlying Kafka Connect source
         */
        private Builder(@Nonnull String name) {
            Objects.requireNonNull(name, "name");

            config = new DebeziumConfig(name, "io.debezium.connector.postgresql.PostgresConnector");
            config.setProperty(CdcSourceP.SEQUENCE_EXTRACTOR_CLASS_PROPERTY, PostgresSequenceExtractor.class.getName());
            config.setProperty(ChangeRecordCdcSourceP.DB_SPECIFIC_EXTRA_FIELDS_PROPERTY, "schema");
            config.setProperty("database.server.name", UuidUtil.newUnsecureUuidString());
            config.setProperty("snapshot.mode", "exported");
        }

        /**
         * IP address or hostname of the database server, has to be specified.
         */
        @Nonnull
        public Builder setDatabaseAddress(@Nonnull String address) {
            config.setProperty("database.hostname", address);
            return this;
        }

        /**
         * Optional port number of the database server, if unspecified defaults
         * to the database specific default port (5432).
         */
        @Nonnull
        public Builder setDatabasePort(int port) {
            config.setProperty("database.port", Integer.toString(port));
            return this;
        }

        /**
         * Database user for connecting to the database server. Has to be
         * specified.
         */
        @Nonnull
        public Builder setDatabaseUser(@Nonnull String user) {
            config.setProperty("database.user", user);
            return this;
        }

        /**
         * Database user password for connecting to the database server. Has to
         * be specified.
         */
        @Nonnull
        public Builder setDatabasePassword(@Nonnull String password) {
            config.setProperty("database.password", password);
            return this;
        }

        /**
         * The name of the PostgreSQL database from which to stream the changes.
         * Has to be set.
         * <p>
         * Currently this source is not capable of monitoring multiple
         * databases, only multiple schemas and/or tables. See white- and
         * black-listing configuration options for those.
         */
        @Nonnull
        public Builder setDatabaseName(@Nonnull String dbName) {
            config.setProperty("database.dbname", dbName);
            return this;
        }

        /**
         * Optional regular expressions that match schema names to be monitored
         * ("schema" is used here to denote logical groups of tables). Any
         * schema name not included in the whitelist will be excluded from
         * monitoring. By default all non-system schemas will be monitored. May
         * not be used with
         * {@link #setSchemaBlacklist(String...) schema blacklist}.
         */
        @Nonnull
        public Builder setSchemaWhitelist(@Nonnull String... schemaNameRegExps) {
            config.setProperty("schema.whitelist", schemaNameRegExps);
            return this;
        }

        /**
         * Optional regular expressions that match schema names to be excluded
         * from monitoring ("schema" is used here to denote logical groups of
         * tables). Any schema name not included in the blacklist will be
         * monitored, with the exception of system schemas. May not be used with
         * {@link #setSchemaWhitelist(String...) schema whitelist}.
         */
        @Nonnull
        public Builder setSchemaBlacklist(@Nonnull String... schemaNameRegExps) {
            config.setProperty("schema.blacklist", schemaNameRegExps);
            return this;
        }

        /**
         * Optional regular expressions that match fully-qualified table
         * identifiers for tables to be monitored; any table not included in the
         * whitelist will be excluded from monitoring. Each identifier is of the
         * form <em>schemaName.tableName</em>. By default the connector will
         * monitor every non-system table in each monitored database. May not be
         * used with {@link #setTableBlacklist(String...) table blacklist}.
         */
        @Nonnull
        public Builder setTableWhitelist(@Nonnull String... tableNameRegExps) {
            config.setProperty("table.whitelist", tableNameRegExps);
            return this;
        }

        /**
         * Optional regular expressions that match fully-qualified table
         * identifiers for tables to be excluded from monitoring; any table not
         * included in the blacklist will be monitored. Each identifier is of
         * the form <em>schemaName.tableName</em>. May not be used with
         * {@link #setTableWhitelist(String...) table whitelist}.
         */
        @Nonnull
        public Builder setTableBlacklist(@Nonnull String... tableNameRegExps) {
            config.setProperty("table.blacklist", tableNameRegExps);
            return this;
        }

        /**
         * Optional regular expressions that match the fully-qualified names of
         * columns that should be excluded from change event message values.
         * Fully-qualified names for columns are of the form
         * <em>schemaName.tableName.columnName</em>.
         */
        @Nonnull
        public Builder setColumnBlacklist(@Nonnull String... columnNameRegExps) {
            config.setProperty("column.blacklist", columnNameRegExps);
            return this;
        }

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
        public Builder setLogicalDecodingPlugIn(@Nonnull String pluginName) {
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
        public Builder setReplicationSlotName(@Nonnull String slotName) {
            config.setProperty("slot.name", slotName);
            return this;
        }

        /**
         * Whether or not to drop the logical replication slot when the
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
        public Builder setReplicationSlotDropOnStop(boolean dropOnStop) {
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
        public Builder setPublicationName(@Nonnull String publicationName) {
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
        public Builder setSslMode(@Nonnull String mode) {
            config.setProperty("database.sslmode", mode);
            return this;
        }

        /**
         * Specifies the (path to the) file containing the SSL Certificate for
         * the database client.
         */
        @Nonnull
        public Builder setSslCertificateFile(@Nonnull String file) {
            config.setProperty("database.sslcert", file);
            return this;
        }

        /**
         * Specifies the (path to the) file containing the SSL private key of
         * the database client.
         */
        @Nonnull
        public Builder setSslKeyFile(@Nonnull String file) {
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
        public Builder setSslKeyFilePassword(@Nonnull String password) {
            config.setProperty("database.sslpassword", password);
            return this;
        }

        /**
         * Specifies the file containing containing SSL certificate authority
         * (CA) certificate(s).
         */
        @Nonnull
        public Builder setSslRootCertificateFile(@Nonnull String file) {
            config.setProperty("database.sslrootcert", file);
            return this;
        }

        /**
         * Specifies how the connector should behave when it detects that the
         * backing database has been shut dow.
         * <p>
         * Defaults to {@link CdcSourceP#DEFAULT_RECONNECT_BEHAVIOR}.
         */
        @Nonnull
        public Builder setReconnectBehavior(@Nonnull RetryStrategy retryStrategy) {
            config.setProperty(CdcSourceP.RECONNECT_BEHAVIOR_PROPERTY, retryStrategy);
            return this;
        }

        /**
         * Specifies if the source's state should be kept or discarded during
         * reconnect attempts to the database. If the state is kept, then
         * database snapshotting should not be repeated and streaming the binlog
         * should resume at the position where it left off. If the state is
         * reset, then the source will behave as if it were its initial start,
         * so will do a database snapshot and will start tailing the binlog
         * where it syncs with the database snapshot's end.
         */
        @Nonnull
        public Builder setShouldStateBeResetOnReconnect(boolean reset) {
            config.setProperty(CdcSourceP.RECONNECT_RESET_STATE_PROPERTY, reset);
            return this;
        }

        /**
         * Specifies how often the connector should confirm processed offsets to
         * the Postgres database's replication slot. For jobs with a processing
         * guarantee this option is ignored, the source confirms the offsets after
         * each state snapshot.
         * <p>
         * If set to <em>zero</em>, the connector will commit the offsets after
         * each batch of change records.
         * <p>
         * If set to a <em>positive</em> value, the commits will be done in the
         * given period.
         * <p>
         * <em>Negative</em> values are not allowed.
         * <p>
         * Defaults to {@link CdcSourceP#DEFAULT_COMMIT_PERIOD_MS}.
         *
         * @since Jet 4.4.1
         */
        @Nonnull
        public Builder setCommitPeriod(long milliseconds) {
            if (milliseconds < 0) {
                throw new IllegalArgumentException("Negative commit period not allowed");
            }
            config.setProperty(CdcSourceP.COMMIT_PERIOD_MILLIS_PROPERTY, milliseconds);
            return this;
        }

        /**
         * Can be used to set any property not explicitly covered by other
         * methods or to override internal properties.
         */
        @Nonnull
        public Builder setCustomProperty(@Nonnull String key, @Nonnull String value) {
            config.setProperty(key, value);
            return this;
        }

        /**
         * Returns the source based on the properties set so far.
         */
        @Nonnull
        public StreamSource<ChangeRecord> build() {
            Properties properties = config.toProperties();
            RULES.check(properties);
            return Sources.streamFromProcessorWithWatermarks(
                    properties.getProperty(CdcSourceP.NAME_PROPERTY),
                    true,
                    eventTimePolicy -> ProcessorMetaSupplier.forceTotalParallelismOne(
                            ProcessorSupplier.of(() -> new ChangeRecordCdcSourceP(properties, eventTimePolicy))));
        }

    }
}
