/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.sql.parser.SqlParser;

import java.util.Properties;

/**
 * Configuration passed to the Calcite.
 * <p>
 * At the moment we do only case-sensitive identifier comparison. It violates SQL standard and negatively affects usability.
 * Case insensitive processing is going to be implemented in future.
 * <p>
 * We also disable materializations because otherwise it leads to NPE for certain queries
 * (see https://github.com/hazelcast/hazelcast/issues/17554).
 */
public final class CalciteConfiguration {

    public static final CalciteConfiguration DEFAULT =
            new CalciteConfiguration(true, Casing.UNCHANGED, Casing.UNCHANGED, Quoting.DOUBLE_QUOTE);

    private final boolean caseSensitive;
    private final Casing unquotedCasing;
    private final Casing quotedCasing;
    private final Quoting quoting;

    private CalciteConfiguration(boolean caseSensitive, Casing unquotedCasing, Casing quotedCasing, Quoting quoting) {
        this.caseSensitive = caseSensitive;
        this.unquotedCasing = unquotedCasing;
        this.quotedCasing = quotedCasing;
        this.quoting = quoting;
    }

    public void toParserConfig(SqlParser.ConfigBuilder configBuilder) {
        configBuilder.setCaseSensitive(caseSensitive);
        configBuilder.setUnquotedCasing(unquotedCasing);
        configBuilder.setQuotedCasing(quotedCasing);
        configBuilder.setQuoting(quoting);
    }

    public CalciteConnectionConfig toConnectionConfig() {
        Properties connectionProperties = new Properties();

        connectionProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.toString(caseSensitive));
        connectionProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), unquotedCasing.toString());
        connectionProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), quotedCasing.toString());
        connectionProperties.put(CalciteConnectionProperty.QUOTING.camelName(), quoting.toString());

        // Disable materializations to avoid NPE described in https://github.com/hazelcast/hazelcast/issues/17554
        connectionProperties.put(CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(), Boolean.toString(false));

        return new CalciteConnectionConfigImpl(connectionProperties);
    }
}
