/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.tpch.model;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.sql.tpch.model.domain.Customer;
import com.hazelcast.sql.tpch.model.domain.LineItem;
import com.hazelcast.sql.tpch.model.domain.Nation;
import com.hazelcast.sql.tpch.model.domain.Order;
import com.hazelcast.sql.tpch.model.domain.Part;
import com.hazelcast.sql.tpch.model.domain.PartSupp;
import com.hazelcast.sql.tpch.model.domain.Region;
import com.hazelcast.sql.tpch.model.domain.Supplier;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Loader for TPC-H benchmark.
 */
public final class ModelLoader {
    public static final String TABLE_PART = "part";
    public static final String TABLE_SUPPLIER = "supplier";
    public static final String TABLE_PARTSUPP = "partsupp";
    public static final String TABLE_CUSTOMER = "customer";
    public static final String TABLE_LINEITEM = "lineitem";
    public static final String TABLE_ORDERS = "orders";
    public static final String TABLE_NATION = "nation";
    public static final String TABLE_REGION = "region";

    private static final String GENERATED_EXT = ".tbl";

    private final ModelConfig config;
    private final HazelcastInstance member;

    private final Set<Long> orderKeys = new HashSet<>();

    private ModelLoader(ModelConfig config, HazelcastInstance member) {
        this.config = config;
        this.member = member;
    }

    public static void load(ModelConfig config, HazelcastInstance member) {
        new ModelLoader(config, member).load();
    }

    private void load() {
        long start = System.currentTimeMillis();

        loadTable(TABLE_REGION);
        loadTable(TABLE_NATION);

        loadTable(TABLE_CUSTOMER);
        loadTable(TABLE_ORDERS);

        loadTable(TABLE_SUPPLIER);
        loadTable(TABLE_PART);
        loadTable(TABLE_PARTSUPP);

        loadTable(TABLE_LINEITEM);

        long dur = System.currentTimeMillis() - start;
        System.out.println(">>> Loaded (" + dur + " ms)");
    }

    private void loadTable(String tableName) {
        String fileName = tableName + GENERATED_EXT;

        Object[] lines;

        try {
            lines = Files.lines(Paths.get(config.getDir(), fileName)).toArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to get file for the table [dir=" + config.getDir() + ", table=" + tableName + ']');
        }

        boolean replicated = TABLE_REGION.equals(tableName) || TABLE_NATION.equals(tableName) || TABLE_SUPPLIER.equals(tableName);

        if (replicated) {
            loadTableReplicated(tableName, lines);
        } else {
            loadTablePartitioned(tableName, lines);
        }
    }

    @SuppressWarnings("unchecked")
    private void loadTableReplicated(String tableName, Object[] lines) {
        try (ProgressTracker tracker = new ProgressTracker(tableName, lines.length)) {
            ReplicatedMap map = member.getReplicatedMap(tableName);

            for (Object line : lines) {
                BiTuple<Object, Object> pair = getKeyValue(tableName, line);

                if (pair == null) {
                    continue;
                }

                map.put(pair.element1, pair.element2);

                tracker.onAdded();
            }
        }
    }

    private void loadTablePartitioned(String tableName, Object[] lines) {
        try (ProgressTracker tracker = new ProgressTracker(tableName, lines.length)) {
            IMap map = member.getMap(tableName);

            try (PartitionedLoader loader = new PartitionedLoader(member, map)) {
                for (Object line : lines) {
                    BiTuple<Object, Object> pair = getKeyValue(tableName, line);

                    if (pair == null) {
                        continue;
                    }

                    loader.add(pair.element1, pair.element2);

                    tracker.onAdded();
                }
            }
        }
    }

    private BiTuple<Object, Object> getKeyValue(String tableName, Object line) {
         String[] elements = ((String)line).split("\\|");
         Arguments args = new Arguments(elements);

         switch (tableName) {
             case TABLE_PART:
                 return createPart(args);

             case TABLE_SUPPLIER:
                 return createSupplier(args);

             case TABLE_PARTSUPP:
                 return createPartSupp(args);

             case TABLE_CUSTOMER:
                 return createCustomer(args);

             case TABLE_LINEITEM:
                 return createLineItem(args);

             case TABLE_ORDERS:
                 return createOrder(args);

             case TABLE_NATION:
                 return createNation(args);

             case TABLE_REGION:
                return createRegion(args);

             default:
                 throw new IllegalArgumentException("Unsupported table: " + tableName);
         }
    }

    private BiTuple<Object, Object> createRegion(Arguments args) {
        long r_regionkey = args.asLong();
        String r_name = args.asString();
        String r_comment = args.asString();

        return BiTuple.of(
            r_regionkey,
            new Region(r_regionkey, r_name, r_comment)
        );
    }

    private BiTuple<Object, Object> createNation(Arguments args) {
        long n_nationkey = args.asLong();
        String n_name = args.asString();
        long n_regionkey = args.asLong();
        String n_comment = args.asString();

        return BiTuple.of(
            n_nationkey,
            new Nation(n_nationkey, n_name, n_regionkey, n_comment)
        );
    }

    private BiTuple<Object, Object> createCustomer(Arguments args) {
        long c_custkey = args.asLong();
        String c_name = args.asString();
        String c_address = args.asString();
        long c_nationkey = args.asLong();
        String c_phone = args.asString();
        BigDecimal c_acctbal = args.asDecimal();
        String c_mktsegment = args.asString();
        String c_comment = args.asString();

        if (ignore(c_custkey)) {
            return null;
        }

        return BiTuple.of(
            c_custkey,
            new Customer(c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)
        );
    }

    private BiTuple<Object, Object> createOrder(Arguments args) {
        long o_orderkey = args.asLong();
        long o_custkey = args.asLong();
        String o_orderstatus = args.asString();
        BigDecimal o_totalprice = args.asDecimal();
        LocalDate o_orderdate = args.asDate();
        String o_orderpriority = args.asString();
        String o_clerk = args.asString();
        int o_shippriority = args.asInt();
        String o_comment = args.asString();

        if (ignore(o_custkey)) {
            return null;
        } else {
            orderKeys.add(o_orderkey);
        }

        return BiTuple.of(
            new Order.Key(o_orderkey, o_custkey),
            new Order(o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment)
        );
    }

    private BiTuple<Object, Object> createSupplier(Arguments args) {
        long s_suppkey = args.asLong();
        String s_name = args.asString();
        String s_address = args.asString();
        long s_nationkey = args.asLong();
        String s_phone = args.asString();
        BigDecimal s_acctbal = args.asDecimal();
        String s_comment = args.asString();

        return BiTuple.of(
            s_suppkey,
            new Supplier(s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment)
        );
    }

    private static BiTuple<Object, Object> createPart(Arguments args) {
        long p_partkey = args.asLong();
        String p_name = args.asString();
        String p_mfgr = args.asString();
        String p_brand = args.asString();
        String p_type = args.asString();
        int p_size = args.asInt();
        String p_container = args.asString();
        BigDecimal p_retailprice = args.asDecimal();
        String p_comment = args.asString();

        return BiTuple.of(
            p_partkey,
            new Part(p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment)
        );
    }

    private BiTuple<Object, Object> createPartSupp(Arguments args) {
        long ps_partkey = args.asLong();
        long ps_suppkey = args.asLong();
        int ps_availqty = args.asInt();
        BigDecimal ps_supplycost = args.asDecimal();
        String ps_comment = args.asString();

        return BiTuple.of(
            new PartSupp.Key(ps_partkey, ps_suppkey),
            new PartSupp(ps_availqty, ps_supplycost, ps_comment)
        );
    }

    private BiTuple<Object, Object> createLineItem(Arguments args) {
        long l_orderkey = args.asLong();
        long l_partkey = args.asLong();
        long l_suppkey = args.asLong();
        long l_linenumber = args.asLong();
        BigDecimal l_quantity = args.asDecimal();
        BigDecimal l_extendedprice = args.asDecimal();
        BigDecimal l_discount = args.asDecimal();
        BigDecimal l_tax = args.asDecimal();
        String l_returnflag = args.asString();
        String l_linestatus = args.asString();
        LocalDate l_shipdate = args.asDate();
        LocalDate l_commitdate = args.asDate();
        LocalDate l_receiptdate = args.asDate();
        String l_shipinstruct = args.asString();
        String l_shipmode = args.asString();
        String l_comment = args.asString();

        if (!orderKeys.contains(l_orderkey)) {
            return null;
        }

        return BiTuple.of(
            new LineItem.Key(l_orderkey, l_partkey, l_linenumber),
            new LineItem(l_suppkey, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate,
                l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment)
        );
    }

    private boolean ignore(long key) {
        return key % config.getDownscale() != 0;
    }

    private static class Arguments {
        private final String[] elements;
        private int idx;

        private Arguments(String[] elements) {
            this.elements = elements;
        }

        private String asString() {
            return elements[idx++];
        }

        private long asLong() {
            return Long.parseLong(asString());
        }

        private int asInt() {
            return Integer.parseInt(asString());
        }

        private BigDecimal asDecimal() {
            return new BigDecimal(asString());
        }

        private LocalDate asDate() {
            return LocalDate.parse(asString());
        }
    }

    private static final class PartitionedLoader implements AutoCloseable {
        private static final int BUFFER_SIZE = 1000;

        private final PartitionService partitionService;
        private final IMap map;
        private final Map<Integer, Map> buffers = new HashMap<>();

        private PartitionedLoader(HazelcastInstance member, IMap map) {
            this.partitionService = member.getPartitionService();
            this.map = map;

            for (Partition partition : partitionService.getPartitions()) {
                buffers.put(partition.getPartitionId(), new HashMap());
            }
        }

        @SuppressWarnings("unchecked")
        private void add(Object key, Object val) {
            int partitionId = partitionService.getPartition(key).getPartitionId();

            Map buffer = buffers.get(partitionId);

            buffer.put(key, val);

            if (buffer.size() >= BUFFER_SIZE) {
                flush(buffer);
            }
        }

        @SuppressWarnings("unchecked")
        private void flush(Map buffer) {
            map.putAll(buffer);

            buffer.clear();
        }

        @Override
        public void close() {
            for (Map buffer : buffers.values()) {
                flush(buffer);
            }
        }
    }

    private static final class ProgressTracker implements AutoCloseable {
        private static final int THROTTLE = 100_000;
        private final String tableName;
        private final int total;
        private int ctr;
        private long startTime;

        private ProgressTracker(String tableName, int total) {
            this.tableName = tableName;
            this.total = total;
        }

        private void onAdded() {
            if (ctr == 0) {
                startTime = System.currentTimeMillis();
            }

            ctr++;

            if (ctr % THROTTLE == 0) {
                print();
            }
        }

        @Override
        public void close() {
            if (ctr % THROTTLE != 0) {
                print();
            }
        }

        private void print() {
            long dur = System.currentTimeMillis() - startTime;

            System.out.println(">>> Loading " + tableName + ": " + ctr + "/" + total + " (" + dur + " ms)");
        }
    }
}
