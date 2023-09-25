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

package com.hazelcast.jet.sql.impl.connector.jdbc.fullscanresultsetstream;

import com.hazelcast.function.SupplierEx;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class FullScanResultSetIteratorTest {
    @Mock
    Connection mockConnection;
    @Mock
    Function<ResultSet, Integer> mockRowMapper;


    @Test
    void testEmptyResultSetMapper() throws SQLException {
        Integer expectedEmptyResult = 1;

        FullScanResultSetIterator<Integer> fullScanResultSetIterator = new FullScanResultSetIterator<>(
                mockConnection,
                "",
                mockRowMapper,
                (SupplierEx<Integer>) () -> expectedEmptyResult
        );
        FullScanResultSetIterator<Integer> spy = Mockito.spy(fullScanResultSetIterator);
        // lazyInit should do nothing
        doNothing().when(spy).lazyInit();
        // getNextItemFromRowMapper should return false to call emptyResultSetMapper
        doReturn(false).when(spy).getNextItemFromRowMapper();

        // hasNext() is true because emptyResultSetMapper has a value
        assertThat(spy.hasNext()).isTrue();
        assertThat(spy.next()).isEqualTo(expectedEmptyResult);

        // Now hasNext() is false because emptyResultSetMapper is consumed
        assertThat(spy.hasNext()).isFalse();

        // After all is consumed iterator must be closed
        verify(spy).close();
    }
}
