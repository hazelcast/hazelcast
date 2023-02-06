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

package com.hazelcast.jet.sql.impl.connector.jdbc;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ExternalNameIdentifiersTest {

    @Test
    public void testParseEmptyString() {
        ExternalNameIdentifiers externalNameIdentifiers = new ExternalNameIdentifiers();
        assertThatThrownBy(() -> externalNameIdentifiers.parseExternalTableName(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No identifiers found");
    }

    @Test
    public void testParseTableName() {
        ExternalNameIdentifiers externalNameIdentifiers = new ExternalNameIdentifiers();
        externalNameIdentifiers.parseExternalTableName("mytable");
        assertNull(externalNameIdentifiers.getCatalog());
        assertNull(externalNameIdentifiers.getSchemaName());
        assertEquals("mytable", externalNameIdentifiers.getTableName());
    }

    @Test
    public void testParseSchemaName() {
        ExternalNameIdentifiers externalNameIdentifiers = new ExternalNameIdentifiers();
        externalNameIdentifiers.parseExternalTableName("myschema.mytable");
        assertNull(externalNameIdentifiers.getCatalog());
        assertEquals("myschema", externalNameIdentifiers.getSchemaName());
        assertEquals("mytable", externalNameIdentifiers.getTableName());
    }

    @Test
    public void testParseCatalogName() {
        ExternalNameIdentifiers externalNameIdentifiers = new ExternalNameIdentifiers();
        externalNameIdentifiers.parseExternalTableName("mycatalog.myschema.mytable");
        assertEquals("mycatalog", externalNameIdentifiers.getCatalog());
        assertEquals("myschema", externalNameIdentifiers.getSchemaName());
        assertEquals("mytable", externalNameIdentifiers.getTableName());
    }

    @Test
    public void testParseQuotedTableName() {
        ExternalNameIdentifiers externalNameIdentifiers = new ExternalNameIdentifiers();
        externalNameIdentifiers.parseExternalTableName("`mytable 1`");
        assertNull(externalNameIdentifiers.getCatalog());
        assertNull(externalNameIdentifiers.getSchemaName());
        assertEquals("mytable 1", externalNameIdentifiers.getTableName());
    }

    @Test
    public void testParseInvalidTableName() {
        ExternalNameIdentifiers externalNameIdentifiers = new ExternalNameIdentifiers();
        assertThatThrownBy(() -> externalNameIdentifiers.parseExternalTableName("`"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Closing quote not found");

    }

    @Test
    public void testParseUnclosedTableName1() {
        ExternalNameIdentifiers externalNameIdentifiers = new ExternalNameIdentifiers();
        assertThatThrownBy(() -> externalNameIdentifiers.parseExternalTableName("`mytable 1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Closing quote not found");

    }

    @Test
    public void testParseQuotedSchemaName() {
        ExternalNameIdentifiers externalNameIdentifiers = new ExternalNameIdentifiers();
        externalNameIdentifiers.parseExternalTableName("`myschema.1 2`.`mytable.1 2`");
        assertNull(externalNameIdentifiers.getCatalog());
        assertEquals("myschema.1 2", externalNameIdentifiers.getSchemaName());
        assertEquals("mytable.1 2", externalNameIdentifiers.getTableName());
    }

    @Test
    public void testParseSchemaSeparatorNotFound() {
        ExternalNameIdentifiers externalNameIdentifiers = new ExternalNameIdentifiers();
        assertThatThrownBy(() -> externalNameIdentifiers.parseExternalTableName("`myschema 1.`mytable 1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Dot separator not found");

    }

    @Test
    public void testParseUnclosedTableName2() {
        ExternalNameIdentifiers externalNameIdentifiers = new ExternalNameIdentifiers();
        assertThatThrownBy(() -> externalNameIdentifiers.parseExternalTableName("`myschema 1`.`mytable 1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Closing quote not found");

    }

    @Test
    public void testParseQuotedCatalogName() {
        ExternalNameIdentifiers externalNameIdentifiers = new ExternalNameIdentifiers();
        externalNameIdentifiers.parseExternalTableName("`mycatalog 1`.`myschema 1`.`mytable 1`");
        assertEquals("mycatalog 1", externalNameIdentifiers.getCatalog());
        assertEquals("myschema 1", externalNameIdentifiers.getSchemaName());
        assertEquals("mytable 1", externalNameIdentifiers.getTableName());
    }

    @Test
    public void testParseCatalogSeparatorNotFound() {
        ExternalNameIdentifiers externalNameIdentifiers = new ExternalNameIdentifiers();
        assertThatThrownBy(() -> externalNameIdentifiers.parseExternalTableName("`mycatalog 1``myschema 1`.`mytable 1`"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Dot separator not found");
    }
}
