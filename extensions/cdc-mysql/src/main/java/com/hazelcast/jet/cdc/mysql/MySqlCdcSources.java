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

package com.hazelcast.jet.cdc.mysql;

import com.hazelcast.jet.annotation.EvolvingApi;
import com.hazelcast.jet.cdc.ChangeRecord;
import com.hazelcast.jet.cdc.impl.CdcSourceP;
import com.hazelcast.jet.cdc.impl.ChangeRecordCdcSourceP;
import com.hazelcast.jet.cdc.impl.DebeziumConfig;
import com.hazelcast.jet.cdc.impl.PropertyRules;
import com.hazelcast.jet.cdc.mysql.impl.MySqlSequenceExtractor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.retry.RetryStrategy;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Properties;

import static com.hazelcast.jet.cdc.impl.CdcSourceP.RECONNECT_BEHAVIOR_PROPERTY;

/**
 * Contains factory methods for creating change data capture sources
 * based on MySQL databases.
 *
 * @since Jet 4.2
 */
@EvolvingApi
public final class MySqlCdcSources {

    private MySqlCdcSources() {
    }

    /**
     * Creates a CDC source that streams change data from a MySQL database to
     * Hazelcast Jet.
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
     * database snapshotting should not be repeated and streaming the binlog
     * should resume at the position where it left off. If the state is reset,
     * then the source will behave as on its initial start, so will do a
     * database snapshot and will start tailing the binlog where it syncs with
     * the database snapshot's end.
     *
     * @param name name of this source, needs to be unique, will be passed to
     *             the underlying Kafka Connect source
     * @return builder that can be used to set source properties and also to
     * construct the source once configuration is done
     */
    @Nonnull
    public static Builder mysql(@Nonnull String name) {
        return new Builder(name);
    }

    /**
     * Builder for configuring a CDC source that streams change data
     * from a MySQL database to Hazelcast Jet.
     */
    public static final class Builder {

        private static final PropertyRules RULES = new PropertyRules()
                .required("database.hostname")
                .required("database.user")
                .required("database.password")
                .required("database.server.name")
                .exclusive("database.whitelist", "database.blacklist")
                .exclusive("table.whitelist", "table.blacklist");

        private final DebeziumConfig config;

        /**
         * @param name name of the source, needs to be unique,
         *             will be passed to the underlying Kafka
         *             Connect source
         */
        private Builder(@Nonnull String name) {
            Objects.requireNonNull(name, "name");

            config = new DebeziumConfig(name, "io.debezium.connector.mysql.MySqlConnector");
            config.setProperty(CdcSourceP.SEQUENCE_EXTRACTOR_CLASS_PROPERTY, MySqlSequenceExtractor.class.getName());
            config.setProperty("include.schema.changes", "false");
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
         * to the database specific default port (3306).
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
         * Logical name that identifies and provides a namespace for the
         * particular database server/cluster being monitored. The logical name
         * should be unique across all other connectors. Only alphanumeric
         * characters and underscores should be used. Has to be specified.
         */
        @Nonnull
        public Builder setClusterName(@Nonnull String cluster) {
            config.setProperty("database.server.name", cluster);
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
        public Builder setDatabaseClientId(int clientId) {
            config.setProperty("database.server.id", clientId);
            return this;
        }

        /**
         * Optional regular expressions that match database names to be
         * monitored; any database name not included in the whitelist will be
         * excluded from monitoring. By default all databases will be monitored.
         * May not be used with {@link #setDatabaseBlacklist(String...) database
         * blacklist}.
         */
        @Nonnull
        public Builder setDatabaseWhitelist(@Nonnull String... dbNameRegExps) {
            config.setProperty("database.whitelist", dbNameRegExps);
            return this;
        }

        /**
         * Optional regular expressions that match database names to be excluded
         * from monitoring; any database name not included in the blacklist will
         * be monitored. May not be used with
         * {@link #setDatabaseWhitelist(String...) database whitelist}.
         */
        @Nonnull
        public Builder setDatabaseBlacklist(@Nonnull String... dbNameRegExps) {
            config.setProperty("database.blacklist", dbNameRegExps);
            return this;
        }

        /**
         * Optional regular expressions that match fully-qualified table
         * identifiers for tables to be monitored; any table not included in the
         * whitelist will be excluded from monitoring. Each identifier is of the
         * form <em>databaseName.tableName</em>. By default the connector will
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
         * the form <em>databaseName.tableName</em>. May not be used with
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
         * <em>databaseName.tableName.columnName</em>, or
         * <em>databaseName.schemaName.tableName.columnName</em>.
         */
        @Nonnull
        public Builder setColumnBlacklist(@Nonnull String... columnNameRegExps) {
            config.setProperty("column.blacklist", columnNameRegExps);
            return this;
        }

        /**
         * Specifies whether to use an encrypted connection to the database. The
         * default is <em>disabled</em>, and specifies to use an unencrypted
         * connection.
         * <p>
         * The <em>preferred</em> option establishes an encrypted connection if
         * the server supports secure connections but falls back to an
         * unencrypted connection otherwise.
         * <p>
         * The <em>required</em> option establishes an encrypted connection but
         * will fail if one cannot be made for any reason.
         * <p>
         * The <em>verify_ca</em> option behaves like <em>required</em> but
         * additionally it verifies the server TLS certificate against the
         * configured Certificate Authority (CA) certificates and will fail if
         * it doesn’t match any valid CA certificates.
         * <p>
         * The <em>verify_identity</em> option behaves like <em>verify_ca</em> but
         * additionally verifies that the server certificate matches the host of
         * the remote connection.
         */
        @Nonnull
        public Builder setSslMode(@Nonnull String mode) {
            config.setProperty("database.ssl.mode", mode);
            return this;
        }

        /**
         * Specifies the (path to the) Java keystore file containing the
         * database client certificate and private key.
         * <p>
         * Can be alternatively specified via the 'javax.net.ssl.keyStore'
         * system or JVM property.
         */
        @Nonnull
        public Builder setSslKeystoreFile(@Nonnull String file) {
            config.setProperty("database.ssl.keystore", file);
            return this;
        }

        /**
         * Password to access the private key from any specified keystore files.
         * <p>
         * This password is used to unlock the keystore file (store password),
         * and to decrypt the private key stored in the keystore (key password)."
         * <p>
         * Can be alternatively specified via the 'javax.net.ssl.keyStorePassword'
         * system or JVM property.
         */
        @Nonnull
        public Builder setSslKeystorePassword(@Nonnull String password) {
            config.setProperty("database.ssl.keystore.password", password);
            return this;
        }

        /**
         * Specifies the (path to the) Java truststore file containing the
         * collection of trusted CA certificates.
         * <p>
         * Can be alternatively specified via the 'javax.net.ssl.trustStore'
         * system or JVM property.
         */
        @Nonnull
        public Builder setSslTruststoreFile(@Nonnull String file) {
            config.setProperty("database.ssl.truststore", file);
            return this;
        }

        /**
         * Password to unlock any specified truststore.
         * <p>
         * Can be alternatively specified via the 'javax.net.ssl.trustStorePassword'
         * system or JVM property.
         */
        @Nonnull
        public Builder setSslTruststorePassword(@Nonnull String password) {
            config.setProperty("database.ssl.truststore.password", password);
            return this;
        }

        /**
         * Specifies how the source should behave when it detects that the
         * backing database has been shut down (read class javadoc for details
         * and special cases).
         * <p>
         * Defaults to {@link CdcSourceP#DEFAULT_RECONNECT_BEHAVIOR}.
         */
        @Nonnull
        public Builder setReconnectBehavior(@Nonnull RetryStrategy retryStrategy) {
            config.setProperty(RECONNECT_BEHAVIOR_PROPERTY, retryStrategy);
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

            properties.setProperty("connect.keep.alive", "true");
            String intervalMs = getKeepAliveIntervalMs(properties);
            properties.setProperty("connect.keep.alive.interval.ms", intervalMs);
            properties.setProperty("connect.timeout.ms", intervalMs);

            return Sources.streamFromProcessorWithWatermarks(
                    properties.getProperty(CdcSourceP.NAME_PROPERTY),
                    true,
                    eventTimePolicy -> ProcessorMetaSupplier.forceTotalParallelismOne(
                            ProcessorSupplier.of(() -> new ChangeRecordCdcSourceP(properties, eventTimePolicy))));
        }

        private static String getKeepAliveIntervalMs(Properties properties) {
            RetryStrategy reconnectBehavior = (RetryStrategy) properties.get(RECONNECT_BEHAVIOR_PROPERTY);
            reconnectBehavior = reconnectBehavior == null ? CdcSourceP.DEFAULT_RECONNECT_BEHAVIOR : reconnectBehavior;
            long waitMs = reconnectBehavior.getIntervalFunction().waitAfterAttempt(1);
            return Long.toString(waitMs / 2);
        }

    }
}
