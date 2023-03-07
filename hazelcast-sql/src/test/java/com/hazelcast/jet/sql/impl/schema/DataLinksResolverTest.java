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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.datalink.DataLink;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DataLinksResolverTest {

    private DataLinksResolver catalog;

    @Mock
    private NodeEngine nodeEngine;

    @Mock
    private DataLinkStorage relationsStorage;

    @Before
    public void before() {
        MockitoAnnotations.openMocks(this);
        catalog = new DataLinksResolver(relationsStorage);
    }

    // region dataLink storage tests

    @Test
    public void when_createDataLink_then_succeeds() {
        // given
        DataLink dataLink = dataLink();
        given(relationsStorage.putIfAbsent(dataLink.name(), dataLink)).willReturn(true);

        // when
        catalog.createDataLink(dataLink, false, false);

        // then
        verify(relationsStorage).putIfAbsent(eq(dataLink.name()), isA(DataLink.class));
    }

    @Test
    public void when_createsDataLinkIfNotExists_then_succeeds() {
        // given
        DataLink dataLink = dataLink();
        given(relationsStorage.putIfAbsent(dataLink.name(), dataLink)).willReturn(true);

        // when
        catalog.createDataLink(dataLink, false, true);

        // then
        verify(relationsStorage).putIfAbsent(eq(dataLink.name()), isA(DataLink.class));
    }

    @Test
    public void when_createsDuplicateDataLinkIfReplace_then_succeeds() {
        // given
        DataLink dataLink = dataLink();

        // when
        catalog.createDataLink(dataLink, true, false);

        // then
        verify(relationsStorage).put(eq(dataLink.name()), isA(DataLink.class));
    }

    @Test
    public void when_createsDuplicateDataLinkIfReplaceAndIfNotExists_then_succeeds() {
        // given
        DataLink dataLink = dataLink();

        // when
        catalog.createDataLink(dataLink, true, true);

        // then
        verify(relationsStorage).putIfAbsent(eq(dataLink.name()), isA(DataLink.class));
    }

    @Test
    public void when_createsDuplicateDataLink_then_throws() {
        // given
        DataLink dataLink = dataLink();
        given(relationsStorage.putIfAbsent(eq(dataLink.name()), isA(DataLink.class))).willReturn(false);

        // when
        // then
        assertThatThrownBy(() -> catalog.createDataLink(dataLink, false, false))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Data link already exists: " + dataLink.name());
    }

    @Test
    public void when_removesNonExistingDataLink_then_throws() {
        // given
        String name = "name";

        given(relationsStorage.removeDataLink(name)).willReturn(null);

        // when
        // then
        assertThatThrownBy(() -> catalog.removeDataLink(name, false))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Data link does not exist: " + name);
    }

    @Test
    public void when_removesNonExistingDataLinkWithIfExists_then_succeeds() {
        // given
        String name = "name";

        given(relationsStorage.removeDataLink(name)).willReturn(null);

        // when
        // then
        catalog.removeDataLink(name, true);
    }

    // endregion

    private static DataLink dataLink() {
        return new DataLink();
    }
}

