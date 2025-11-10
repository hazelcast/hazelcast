/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.sql.SqlStatement;

import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;

/**
 * SQL service configuration.
 */
public class SqlConfig {

    /** Default timeout in milliseconds that is applied to statements without explicit timeout. */
    public static final int DEFAULT_STATEMENT_TIMEOUT_MILLIS = 0;

    /** Timeout in milliseconds that is applied to statements without an explicit timeout. */
    private long statementTimeoutMillis = DEFAULT_STATEMENT_TIMEOUT_MILLIS;

    /** Whether persistence is enabled or not, false by default. */
    private boolean catalogPersistenceEnabled;

    /** Restricting the use of reflections through the utilization of black and white lists. */
    private JavaSerializationFilterConfig javaReflectionFilterConfig;

    /**
     * Gets the timeout in milliseconds that is applied to statements without an explicit timeout.
     *
     * @return timeout in milliseconds
     */
    public long getStatementTimeoutMillis() {
        return statementTimeoutMillis;
    }

    /**
     * Sets the timeout in milliseconds that is applied to statements without an explicit timeout.
     * <p>
     * It is possible to set a timeout through the {@link SqlStatement#setTimeoutMillis(long)} method. If the statement
     * timeout is not set, then the value of this parameter will be used.
     * <p>
     * Zero value means no timeout. Negative values are prohibited.
     * <p>
     * Defaults to {@link #DEFAULT_STATEMENT_TIMEOUT_MILLIS}.
     *
     * @see SqlStatement#setTimeoutMillis(long)
     * @param statementTimeoutMillis timeout in milliseconds
     * @return this instance for chaining
     */
    public SqlConfig setStatementTimeoutMillis(long statementTimeoutMillis) {
        checkNotNegative(statementTimeoutMillis, "Timeout cannot be negative");

        this.statementTimeoutMillis = statementTimeoutMillis;

        return this;
    }

    /**
     * Returns {@code true} if persistence is enabled for SQL Catalog.
     *
     * @return {@code true} if persistence is enabled for SQL Catalog
     */
    public boolean isCatalogPersistenceEnabled() {
        return catalogPersistenceEnabled;
    }

    /**
     * Sets whether SQL Catalog persistence is enabled for the node. With SQL
     * Catalog persistence enabled you can restart the whole cluster without
     * losing schema definition objects (such as MAPPINGs, TYPEs, VIEWs and DATA
     * CONNECTIONs). The feature is implemented on top of the Hot Restart
     * feature of Hazelcast which persists the data to disk. If enabled, you
     * have to also configure Hot Restart. Feature is disabled by default. If
     * you enable this option in open-source, the member will fail to start, you
     * need Enterprise to run it and obtain a license from Hazelcast.
     *
     * @param catalogPersistenceEnabled to enable or disable persistence for SQL Catalog
     * @return this config instance
     */
    public SqlConfig setCatalogPersistenceEnabled(final boolean catalogPersistenceEnabled) {
        this.catalogPersistenceEnabled = catalogPersistenceEnabled;
        return this;
    }

    /**
     * @return the reflection filter, the configuration of restrictions on class usage in SQL mapping and UDT.
     */
    public JavaSerializationFilterConfig getJavaReflectionFilterConfig() {
        return javaReflectionFilterConfig;
    }

    /**
     * Allows to configure reflection protection filter.
     * Enable the configuration of restrictions on class usage in SQL mapping and UDT.
     *
     * @param javaReflectionFilterConfig the filter config to set (may be {@code null})
     */
    public void setJavaReflectionFilterConfig(JavaSerializationFilterConfig javaReflectionFilterConfig) {
        this.javaReflectionFilterConfig = javaReflectionFilterConfig;
    }

    @Override
    public String toString() {
        return "SqlConfig{"
            + "statementTimeoutMillis=" + statementTimeoutMillis
            + ", catalogPersistenceEnabled=" + catalogPersistenceEnabled
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SqlConfig sqlConfig = (SqlConfig) o;
        return statementTimeoutMillis == sqlConfig.statementTimeoutMillis
                && catalogPersistenceEnabled == sqlConfig.catalogPersistenceEnabled
                && Objects.equals(javaReflectionFilterConfig, sqlConfig.javaReflectionFilterConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(statementTimeoutMillis, catalogPersistenceEnabled, javaReflectionFilterConfig);
    }
}
