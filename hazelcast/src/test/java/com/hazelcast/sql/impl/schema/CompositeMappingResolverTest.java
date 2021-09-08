/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.schema;

import com.hazelcast.sql.impl.schema.MappingResolver.CompositeMappingResolver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;

@RunWith(MockitoJUnitRunner.class)
public class CompositeMappingResolverTest {

    private MappingResolver resolver;

    @Mock
    private MappingResolver firstResolver;

    @Mock
    private MappingResolver secondResolver;

    @Before
    public void setUp() {
        resolver = new CompositeMappingResolver(firstResolver, secondResolver);
    }

    @Test
    public void test_resolve() {
        Mapping mapping = new Mapping("name", "external-name", "Type", emptyList(), emptyMap());

        given(firstResolver.resolve(anyString())).willReturn(null);
        given(firstResolver.resolve(mapping.name())).willReturn(mapping);

        Mapping resolved = resolver.resolve(mapping.name());

        assertThat(resolved).isEqualTo(mapping);
    }
}
