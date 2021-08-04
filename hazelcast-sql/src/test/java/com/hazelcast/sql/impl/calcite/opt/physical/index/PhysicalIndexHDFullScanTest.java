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

package com.hazelcast.sql.impl.calcite.opt.physical.index;

import com.hazelcast.config.IndexType;
import com.hazelcast.sql.impl.calcite.opt.OptimizerTestSupport;
import com.hazelcast.sql.impl.calcite.schema.HazelcastSchema;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.apache.calcite.schema.Table;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for HD full scans.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(ParallelJVMTest.class)
public class PhysicalIndexHDFullScanTest extends IndexOptimizerTestSupport {

    private HazelcastSchema schema;

    @Override
    protected HazelcastSchema createDefaultSchema() {
        assertNotNull("Schema is not set", schema);

        return schema;
    }

    private void createSchema(IndexType... indexTypes) {
        Map<String, Table> tableMap = new HashMap<>();

        Set<String> indexNames = new HashSet<>();
        List<MapTableIndex> indexes = new ArrayList<>();

        for (IndexType indexType : indexTypes) {
            String indexName = indexType.name();

            if (!indexNames.add(indexName)) {
                throw new RuntimeException("Index with the given type is already registered: " + indexType);
            }

            MapTableIndex index = new MapTableIndex(indexName, indexType, 1, singletonList(0), singletonList(INT));

            indexes.add(index);
        }

        HazelcastTable pTable = OptimizerTestSupport.partitionedTable(
                "p",
                OptimizerTestSupport.fields("ret", INT, "f", INT),
                indexes,
                100,
                true
        );

        tableMap.put("p", pTable);

        schema = new HazelcastSchema(tableMap);
    }

    @Test
    public void test_sorted() {

        createSchema(IndexType.SORTED);
        checkIndexFullScan(IndexType.SORTED);

        createSchema(IndexType.SORTED, IndexType.HASH);
        checkIndexFullScan(IndexType.SORTED);

        createSchema(IndexType.SORTED, IndexType.BITMAP);
        checkIndexFullScan(IndexType.SORTED);

        createSchema(IndexType.SORTED, IndexType.HASH, IndexType.BITMAP);
        checkIndexFullScan(IndexType.SORTED);

        createSchema(IndexType.SORTED, IndexType.BITMAP, IndexType.HASH);
        checkIndexFullScan(IndexType.SORTED);
    }

    @Test
    public void test_hash() {
        createSchema(IndexType.HASH);
        checkIndexFullScan(IndexType.HASH);

        createSchema(IndexType.HASH, IndexType.SORTED);
        checkIndexFullScan(IndexType.SORTED);

        createSchema(IndexType.HASH, IndexType.BITMAP);
        checkIndexFullScan(IndexType.HASH);

        createSchema(IndexType.HASH, IndexType.SORTED, IndexType.BITMAP);
        checkIndexFullScan(IndexType.SORTED);

        createSchema(IndexType.HASH, IndexType.BITMAP, IndexType.SORTED);
        checkIndexFullScan(IndexType.SORTED);
    }

    @Test
    public void test_bitmap() {
        createSchema(IndexType.BITMAP);
        checkNoIndex("SELECT ret FROM p");

        createSchema(IndexType.BITMAP, IndexType.SORTED);
        checkIndexFullScan(IndexType.SORTED);

        createSchema(IndexType.BITMAP, IndexType.HASH);
        checkIndexFullScan(IndexType.HASH);

        createSchema(IndexType.BITMAP, IndexType.SORTED, IndexType.HASH);
        checkIndexFullScan(IndexType.SORTED);

        createSchema(IndexType.BITMAP, IndexType.HASH, IndexType.SORTED);
        checkIndexFullScan(IndexType.SORTED);
    }

    private void checkIndexFullScan(IndexType expectedIndexType) {
        checkIndex("SELECT ret FROM p", expectedIndexType.name(), "null", "null");
    }
}
