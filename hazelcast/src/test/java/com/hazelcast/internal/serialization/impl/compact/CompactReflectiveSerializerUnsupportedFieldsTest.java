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

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompactReflectiveSerializerUnsupportedFieldsTest {

    private final SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();
    private InternalSerializationService service;

    @Before
    public void createSerializationService() {
        CompactSerializationConfig compactSerializationConfig = new CompactSerializationConfig();
        compactSerializationConfig.setEnabled(true);
        service = new DefaultSerializationServiceBuilder()
                .setSchemaService(schemaService)
                .setConfig(new SerializationConfig().setCompactSerializationConfig(compactSerializationConfig))
                .build();
    }

    @Test
    public void shouldThrowWhileSerializingClassWithCharField() {
        CharClass charClass = new CharClass('x');
        assertThatThrownBy(() -> service.toData(charClass))
                .hasRootCauseInstanceOf(HazelcastSerializationException.class)
                .hasStackTraceContaining("does not support fields of type 'char'");
    }

    @Test
    public void shouldThrowWhileSerializingClassWithCharacterField() {
        CharacterClass characterClass = new CharacterClass('x');
        assertThatThrownBy(() -> service.toData(characterClass))
                .hasRootCauseInstanceOf(HazelcastSerializationException.class)
                .hasStackTraceContaining("does not support fields of type 'Character'");
    }

    @Test
    public void shouldThrowWhileSerializingClassWithCharArrayField() {
        CharArrayClass charArrayClass = new CharArrayClass(new char[]{'x'});
        assertThatThrownBy(() -> service.toData(charArrayClass))
                .hasRootCauseInstanceOf(HazelcastSerializationException.class)
                .hasStackTraceContaining("does not support fields of type 'char[]'");
    }

    @Test
    public void shouldThrowWhileSerializingClassWithCharacterArrayField() {
        CharacterArrayClass characterArrayClass = new CharacterArrayClass(new Character[]{'x'});
        assertThatThrownBy(() -> service.toData(characterArrayClass))
                .hasRootCauseInstanceOf(HazelcastSerializationException.class)
                .hasStackTraceContaining("does not support fields of type 'Character[]'");
    }

    class CharClass {
        private final char c;

        CharClass(char c) {
            this.c = c;
        }
    }

    class CharacterClass {
        private final Character c;

        CharacterClass(Character c) {
            this.c = c;
        }
    }

    class CharArrayClass {
        private final char[] ca;

        CharArrayClass(char[] ca) {
            this.ca = ca;
        }
    }

    class CharacterArrayClass {
        private final Character[] ca;

        CharacterArrayClass(Character[] ca) {
            this.ca = ca;
        }
    }
}
