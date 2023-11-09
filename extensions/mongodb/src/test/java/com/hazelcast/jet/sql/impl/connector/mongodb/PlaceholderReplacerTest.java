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
package com.hazelcast.jet.sql.impl.connector.mongodb;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class PlaceholderReplacerTest {

    @Test
    public void replaces_dynamic_param() {
        // given
        Document embedded = new Document("test", "<!DynamicParameter(1)!>");
        Document doc = new Document("<!DynamicParameter(0)!>", embedded);

        List<Object> arguments = asList("jeden", "dwa");

        // when
        Document result = PlaceholderReplacer.replacePlaceholders(doc, evalContext(arguments), (Object[]) null,
                null, false);

        // then
        assertThat(result).isInstanceOf(Document.class);

        Document expected = new Document("jeden", new Document("test", "dwa"));
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void replaces_input_ref_takes_from_input() {
        // given
        Document embedded = new Document("test", "<!InputRef(1)!>");
        Document doc = new Document("<!InputRef(0)!>", embedded);

        List<Object> arguments = Collections.emptyList();

        // when
        Object[] inputs = {"jeden", "dwa"};
        String[] externalNames = {"jeden", "dwa"};
        JetSqlRow inputRow = new JetSqlRow(getInternalSerializationService(), inputs);
        Bson result = PlaceholderReplacer.replacePlaceholders(doc, evalContext(arguments), inputRow, externalNames, true);

        // then
        assertThat(result).isInstanceOf(Document.class);

        Document expected = new Document("jeden", new Document("test", "dwa"));
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void replaces_input_ref_makes_reference() {
        // given
        Document embedded = new Document("test", "<!InputRef(1)!>");
        Document doc = new Document("<!InputRef(0)!>", embedded);

        List<Object> arguments = Collections.emptyList();

        // when
        Object[] inputs = {"jeden", "dwa"};
        String[] externalNames = {"jeden", "col2"};
        JetSqlRow inputRow = new JetSqlRow(getInternalSerializationService(), inputs);
        Bson result = PlaceholderReplacer.replacePlaceholders(doc, evalContext(arguments), inputRow, externalNames, false);

        // then
        assertThat(result).isInstanceOf(Document.class);

        Document expected = new Document("jeden", new Document("test", "$col2"));
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void replaces_mixed() {
        // given
        Document embedded = new Document("<!InputRef(1)!>", "<!DynamicParameter(0)!>");
        Document doc = new Document("<!InputRef(0)!>", embedded);

        // when
        List<Object> arguments = singletonList("dwa");
        Object[] inputs = {"jeden", "test"};
        String[] externalNames = {"col1", "col2"};
        JetSqlRow inputRow = new JetSqlRow(getInternalSerializationService(), inputs);
        Bson result = PlaceholderReplacer.replacePlaceholders(doc, evalContext(arguments), inputRow, externalNames, true);

        // then
        assertThat(result).isInstanceOf(Document.class);

        Document expected = new Document("col1", new Document("col2", "dwa"));
        assertThat(result).isEqualTo(expected);
    }

    private ExpressionEvalContext evalContext(List<Object> arguments) {
        return ExpressionEvalContext.createContext(
                arguments,
                mock(NodeEngine.class),
                getInternalSerializationService(),
                null
        );
    }

    private static InternalSerializationService getInternalSerializationService() {
        return new DefaultSerializationServiceBuilder().build();
    }

}
