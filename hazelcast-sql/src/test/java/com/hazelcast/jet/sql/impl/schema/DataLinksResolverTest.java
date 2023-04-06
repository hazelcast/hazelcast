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

import com.hazelcast.datalink.impl.DataLinkServiceImpl;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.datalink.DataLinkCatalogEntry;
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

    private DataLinksResolver dataLinksResolver;

    @Mock
    DataLinkServiceImpl dataLinkService;

    @Mock
    private DataLinkStorage relationsStorage;

    @Before
    public void before() {
        MockitoAnnotations.openMocks(this);
        dataLinksResolver = new DataLinksResolver(dataLinkService, relationsStorage);
    }

    // region dataLink storage tests

    @Test
    public void when_createDataLink_then_succeeds() {
        // given
        DataLinkCatalogEntry dataLinkCatalogEntry = dataLink();
        given(relationsStorage.putIfAbsent(dataLinkCatalogEntry.name(), dataLinkCatalogEntry)).willReturn(true);

        // when
        dataLinksResolver.createDataLink(dataLinkCatalogEntry, false, false);

        // then
        verify(relationsStorage).putIfAbsent(eq(dataLinkCatalogEntry.name()), isA(DataLinkCatalogEntry.class));
    }

    @Test
    public void when_createsDataLinkIfNotExists_then_succeeds() {
        // given
        DataLinkCatalogEntry dataLinkCatalogEntry = dataLink();
        given(relationsStorage.putIfAbsent(dataLinkCatalogEntry.name(), dataLinkCatalogEntry)).willReturn(true);

        // when
        dataLinksResolver.createDataLink(dataLinkCatalogEntry, false, true);

        // then
        verify(relationsStorage).putIfAbsent(eq(dataLinkCatalogEntry.name()), isA(DataLinkCatalogEntry.class));
    }

    @Test
    public void when_createsDuplicateDataLinkIfReplace_then_succeeds() {
        // given
        DataLinkCatalogEntry dataLinkCatalogEntry = dataLink();

        // when
        dataLinksResolver.createDataLink(dataLinkCatalogEntry, true, false);

        // then
        verify(relationsStorage).put(eq(dataLinkCatalogEntry.name()), isA(DataLinkCatalogEntry.class));
    }

    @Test
    public void when_createsDuplicateDataLinkIfReplaceAndIfNotExists_then_succeeds() {
        // given
        DataLinkCatalogEntry dataLinkCatalogEntry = dataLink();

        // when
        dataLinksResolver.createDataLink(dataLinkCatalogEntry, false, true);

        // then
        verify(relationsStorage).putIfAbsent(eq(dataLinkCatalogEntry.name()), isA(DataLinkCatalogEntry.class));
    }

    @Test
    public void when_createsDuplicateDataLink_then_throws() {
        // given
        DataLinkCatalogEntry dataLinkCatalogEntry = dataLink();
        given(relationsStorage.putIfAbsent(eq(dataLinkCatalogEntry.name()), isA(DataLinkCatalogEntry.class))).willReturn(false);

        // when
        // then
        assertThatThrownBy(() -> dataLinksResolver.createDataLink(dataLinkCatalogEntry, false, false))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Data link already exists: " + dataLinkCatalogEntry.name());
    }

    @Test
    public void when_removesNonExistingDataLink_then_throws() {
        // given
        String name = "name";

        given(relationsStorage.removeDataLink(name)).willReturn(false);

        // when
        // then
        assertThatThrownBy(() -> dataLinksResolver.removeDataLink(name, false))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Data link does not exist: " + name);
    }

    @Test
    public void when_removesNonExistingDataLinkWithIfExists_then_succeeds() {
        // given
        String name = "name";

        given(relationsStorage.removeDataLink(name)).willReturn(false);

        // when
        // then
        dataLinksResolver.removeDataLink(name, true);
    }

    // endregion

    private static DataLinkCatalogEntry dataLink() {
        return new DataLinkCatalogEntry();
    }
}
