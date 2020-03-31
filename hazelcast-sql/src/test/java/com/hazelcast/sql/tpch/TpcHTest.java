/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.tpch;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.partition.strategy.DeclarativePartitioningStrategy;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.SqlCursorImpl;
import com.hazelcast.sql.impl.calcite.OptimizerConfig;
import com.hazelcast.sql.impl.calcite.OptimizerContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.support.SqlTestSupport;
import com.hazelcast.sql.tpch.model.ModelConfig;
import com.hazelcast.sql.tpch.model.ModelLoader;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.fail;

/**
 * Tests of TPC-H benchmark queries.
 */
@Ignore
@SuppressWarnings({"checkstyle:OperatorWrap", "unused"})
public class TpcHTest extends SqlTestSupport {
    // TODO: Externalize data location.
    private static final String DATA_DIR = "/home/devozerov/code/tpch/2.18.0_rc2/dbgen";
    private static final int DOWNSCALE = 10;

    private static TestHazelcastInstanceFactory factory;
    private static HazelcastInstance member;

    @BeforeClass
    public static void beforeClass() {
        factory = new TestHazelcastInstanceFactory(2);

        member = factory.newHazelcastInstance(prepareConfig());
        factory.newHazelcastInstance(prepareConfig());

        // TODO: This is a very serious issue! Maps are started lazily.
        member.getMap("part");
        member.getReplicatedMap("supplier");
        member.getMap("partsupp");
        member.getMap("customer");
        member.getMap("lineitem");
        member.getMap("orders");
        member.getReplicatedMap("nation");
        member.getReplicatedMap("region");

        ModelConfig modelConfig = ModelConfig.builder().setDirectory(DATA_DIR).setDownscale(DOWNSCALE).build();
        ModelLoader.load(modelConfig, member);
    }

    private static Config prepareConfig() {
        Config config = new Config();

        // Common dictionaries.
        config.addReplicatedMapConfig(new ReplicatedMapConfig("nation"));
        config.addReplicatedMapConfig(new ReplicatedMapConfig("region"));

        // Customer-order
        config.addMapConfig(new MapConfig("customer"));
        config.addMapConfig(new MapConfig("orders").setPartitioningStrategyConfig(partitioning("o_custkey")));

        // Part-supplier
        config.addReplicatedMapConfig(new ReplicatedMapConfig("supplier"));
        config.addMapConfig(new MapConfig("part"));
        config.addMapConfig(new MapConfig("partsupp").setPartitioningStrategyConfig(partitioning("ps_partkey")));

        // Line item
        config.addMapConfig(new MapConfig("lineitem").setPartitioningStrategyConfig(partitioning("l_partkey")));

        return config;
    }

    @SuppressWarnings("rawtypes")
    private static PartitioningStrategyConfig partitioning(String fieldName) {
        DeclarativePartitioningStrategy strategy = new DeclarativePartitioningStrategy().setField(fieldName);

        return new PartitioningStrategyConfig().setPartitioningStrategy(strategy);
    }

    @AfterClass
    public static void afterClass() {
        if (factory != null) {
            factory.shutdownAll();
        }
    }

    @Test
    public void testQ1() {
        LocalDate date = LocalDate.parse("1998-12-01").minusDays(90);

        List<SqlRow> rows = execute(
        "select\n" +
            "    l_returnflag,\n" +
            "    l_linestatus,\n" +
            "    sum(l_quantity) as sum_qty,\n" +
            "    sum(l_extendedprice) as sum_base_price,\n" +
            "    sum(l_extendedprice*(1-l_discount)) as sum_disc_price,\n" +
            "    sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,\n" +
            "    avg(l_quantity) as avg_qty,\n" +
            "    avg(l_extendedprice) as avg_price,\n" +
            "    avg(l_discount) as avg_disc,\n" +
            "    count(*) as count_order\n" +
            "from\n" +
            "    lineitem\n" +
            "where\n" +
            "    l_shipdate <= ?\n" +
            "group by\n" +
            "    l_returnflag,\n" +
            "    l_linestatus\n" +
            "order by\n" +
            "    l_returnflag,\n" +
            "    l_linestatus"
        , -1, date);
    }

    @Test
    public void testQ2() {
        int size = 15;
        String type = "BRASS";
        String region = "EUROPE";

        // TODO: NLJ is generate at the moment because join order is [part, supplier, ...] and there is no condition
        //  between them, hence we treat the relation as cross-join.

        // TODO: Notice broadcasts in the plan. This is because we do not have a cost model for exchanges yet, so the
        //  planner doesn't know what to pick.

        // TODO: Another problem is that (RANDOM, PARTITIONED) join pair does have a strategy to produce (UNICAST, UNICAST).
        //  It only produces (UNICAST, NONE) AND (NONE, BROADCAST)

        // TODO: If you look carefully at the top project, you will see that there are a number of redundant fields.
        //  Specifically: 1, 2 (part); 4, 5 (supplier); 11, 12 (partsupp); 14, 15 (nation); 17, 18 (region)
        //  The reason for this is that we cannot pushdown the filter past join. We do have ProjectJoinTransposeRule,
        //  but it seems that it is not fired properly!? Perhaps we need to add project to Join, the same way we did
        //  that for scan?

        List<SqlRow> rows = execute(
            "select\n" +
                "    s.s_acctbal,\n" +
                "    s.s_name,\n" +
                "    n.n_name,\n" +
                "    p.__key,\n" +
                "    p.p_mfgr,\n" +
                "    s.s_address,\n" +
                "    s.s_phone,\n" +
                "    s.s_comment\n" +
                "from\n" +
                "    part p,\n" +
                "    supplier s,\n" +
                "    partsupp ps,\n" +
                "    nation n,\n" +
                "    region r\n" +
                "where\n" +
                "    p.__key = ps.ps_partkey\n" +
                "    and s.s_suppkey = ps.ps_suppkey\n" +
                "    and p.p_size = ?\n" +
                "    and p.p_type like ?\n" +
                "    and s.s_nationkey = n.n_nationkey\n" +
                "    and n.n_regionkey = r.r_regionkey\n" +
                "    and r.r_name = ?\n" +
                "    and ps.ps_supplycost = (        \n" +
                "        select \n" +
                "            min(ps2.ps_supplycost)\n" +
                "        from\n" +
                "            partsupp ps2, supplier s2,\n" +
                "            nation n2, region r2\n" +
                "        where\n" +
                "            p.__key = ps2.ps_partkey\n" +
                "            and s2.s_suppkey = ps2.ps_suppkey\n" +
                "            and s2.s_nationkey = n2.n_nationkey\n" +
                "            and n2.n_regionkey = r2.r_regionkey\n" +
                "            and r2.r_name = ?\n" +
                "    )\n" +
                "order by\n" +
                "    s.s_acctbal desc,\n" +
                "    n.n_name,\n" +
                "    s.s_name,\n" +
                "    p.__key"
        , 100, size, type, region, region);
    }

    @Test
    public void testQ3() {
        String segment = "BUILDING";
        LocalDate date = LocalDate.parse("1995-03-15");

        List<SqlRow> rows = execute(
            "select\n" +
                "    l.l_orderkey,\n" +
                "    sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,\n" +
                "    o.o_orderdate,\n" +
                "    o.o_shippriority\n" +
                "from\n" +
                "    customer c,\n" +
                "    orders o,\n" +
                "    lineitem l\n" +
                "where\n" +
                "    c.c_mktsegment = ?\n" +
                "    and c.__key = o.o_custkey\n" +
                "    and l.l_orderkey = o.o_orderkey\n" +
                "    and o.o_orderdate < ?\n" +
                "    and l.l_shipdate > ?\n" +
                "group by\n" +
                "    l.l_orderkey,\n" +
                "    o.o_orderdate,\n" +
                "    o.o_shippriority\n" +
                "order by\n" +
                "    revenue desc,\n" +
                "    o.o_orderdate"
        , 10, segment, date, date);
    }

    @Test
    public void testQ4() {
        LocalDate date = LocalDate.parse("1993-07-01");

        List<SqlRow> rows = execute(
            "select\n" +
                "    o.o_orderpriority,\n" +
                "    count(*) as order_count\n" +
                "from\n" +
                "    orders o\n" +
                "where\n" +
                "    o.o_orderdate >= ?\n" +
                "    and o.o_orderdate < ?\n" +
                "    and exists (\n" +
                "        select\n" +
                "            *\n" +
                "        from\n" +
                "            lineitem l\n" +
                "        where\n" +
                "            l.l_orderkey = o.o_orderkey\n" +
                "            and l.l_commitdate < l.l_receiptdate\n" +
                "    )\n" +
                "group by\n" +
                "    o.o_orderpriority\n" +
                "order by\n" +
                "    o.o_orderpriority"
        , -1, date, date.plusMonths(3));
    }

    @Test
    public void testQ5() {
        String region = "ASIA";
        LocalDate date = LocalDate.parse("1994-01-01");

        List<SqlRow> rows = execute(
            "select\n" +
                "    n.n_name,\n" +
                "    sum(l.l_extendedprice * (1 - l.l_discount)) as revenue\n" +
                "from\n" +
                "    customer c,\n" +
                "    orders o,\n" +
                "    lineitem l,\n" +
                "    supplier s,\n" +
                "    nation n,\n" +
                "    region r\n" +
                "where\n" +
                "    c.__key = o.o_custkey\n" +
                "    and l.l_orderkey = o.o_orderkey\n" +
                "    and l.l_suppkey = s.s_suppkey\n" +
                "    and c.c_nationkey = s.s_nationkey\n" +
                "    and s.s_nationkey = n.n_nationkey\n" +
                "    and n.n_regionkey = r.r_regionkey\n" +
                "    and r.r_name = ?\n" +
                "    and o.o_orderdate >= ?\n" +
                "    and o.o_orderdate < ?\n" +
                "group by\n" +
                "    n.n_name\n" +
                "order by\n" +
                "    revenue desc"
        , -1, region, date, date.plusYears(1));
    }

    @Test
    public void testQ6() {
        LocalDate date = LocalDate.parse("1994-01-01");
        BigDecimal discount = new BigDecimal("0.06");
        BigDecimal quantity = new BigDecimal("24");

        List<SqlRow> rows = execute(
            "select\n" +
                "    sum(l.l_extendedprice * l.l_discount) as revenue\n" +
                "from\n" +
                "    lineitem l\n" +
                "where\n" +
                "    l.l_shipdate >= ?\n" +
                "    and l.l_shipdate < ?\n" +
                "    and l.l_discount between (? - 0.01) AND (? + 0.01)\n" +
                "    and l.l_quantity < ?"
        , -1, date, date.plusYears(1), discount, discount, quantity);
    }

    @Test
    public void testQ7() {
        String nation1 = "FRANCE";
        String nation2 = "GERMANY";

        List<SqlRow> rows = execute(
            "select\n" +
                "    supp_nation,\n" +
                "    cust_nation,\n" +
                "    l_year, sum(volume) as revenue\n" +
                "from (\n" +
                "    select\n" +
                "        n1.n_name as supp_nation,\n" +
                "        n2.n_name as cust_nation,\n" +
                "        extract(year from l.l_shipdate) as l_year,\n" +
                "        l.l_extendedprice * (1 - l.l_discount) as volume\n" +
                "    from\n" +
                "        supplier s,\n" +
                "        lineitem l,\n" +
                "        orders o,\n" +
                "        customer c,\n" +
                "        nation n1,\n" +
                "        nation n2\n" +
                "    where\n" +
                "        s.s_suppkey = l.l_suppkey\n" +
                "        and o.o_orderkey = l.l_orderkey\n" +
                "        and c.__key = o.o_custkey\n" +
                "        and s.s_nationkey = n1.n_nationkey\n" +
                "        and c.c_nationkey = n2.n_nationkey\n" +
                "        and (\n" +
                "            (n1.n_name = ? and n2.n_name = ?)\n" +
                "            or (n1.n_name = ? and n2.n_name = ?)\n" +
                "        )\n" +
                "        and l.l_shipdate between date '1995-01-01' and date '1996-12-31'\n" +
                "    ) as shipping\n" +
                "group by\n" +
                "    supp_nation,\n" +
                "    cust_nation,\n" +
                "    l_year\n" +
                "order by\n" +
                "    supp_nation,\n" +
                "    cust_nation,\n" +
                "    l_year"
        , -1, nation1, nation2, nation2, nation1);
    }

    @Test
    public void testQ8() {
        String region = "AMERICA";
        String type = "ECONOMY ANODIZED STEEL";

        List<SqlRow> rows = execute(
            "select\n" +
                "    o_year,\n" +
                "    sum(case\n" +
                "        when nation = 'BRAZIL'\n" + // TODO: Calcite doesn't support ? in CASE, so inline
                "        then volume\n" +
                "        else 0.0\n" + // TODO: Had to adjust 0 -> 0.0 to avoid numerous ClassCast problems, BigDecimal-int clash
                "    end) / sum(volume) as mkt_share\n" +
                "from (\n" +
                "    select\n" +
                "        extract(year from o.o_orderdate) as o_year,\n" +
                "        l.l_extendedprice * (1 - l.l_discount) as volume,\n" +
                "        n2.n_name as nation\n" +
                "    from\n" +
                "        part p,\n" +
                "        supplier s,\n" +
                "        lineitem l,\n" +
                "        orders o,\n" +
                "        customer c,\n" +
                "        nation n1,\n" +
                "        nation n2,\n" +
                "        region r\n" +
                "    where\n" +
                "        p.__key = l.l_partkey\n" +
                "        and s.s_suppkey = l.l_suppkey\n" +
                "        and l.l_orderkey = o.o_orderkey\n" +
                "        and o.o_custkey = c.__key\n" +
                "        and c.c_nationkey = n1.n_nationkey\n" +
                "        and n1.n_regionkey = r.r_regionkey\n" +
                "        and r.r_name = ?\n" +
                "        and s.s_nationkey = n2.n_nationkey\n" +
                "        and o.o_orderdate between date '1995-01-01' and date '1996-12-31'\n" +
                "        and p.p_type = ?\n" +
                "    ) as all_nations\n" +
                "group by\n" +
                "    o_year\n" +
                "order by\n" +
                "    o_year"
        , -1, region, type);
    }

    @Test
    public void testQ9() {
        String color = "%green%";

        List<SqlRow> rows = execute(
            "select\n" +
                "    nation,\n" +
                "    o_year,\n" +
                "    sum(amount) as sum_profit\n" +
                "from (\n" +
                "    select\n" +
                "        n.n_name as nation,\n" +
                "        extract(year from o.o_orderdate) as o_year,\n" +
                "        l.l_extendedprice * (1 - l.l_discount) - ps.ps_supplycost * l.l_quantity as amount\n" +
                "    from\n" +
                "        part p,\n" +
                "        supplier s,\n" +
                "        lineitem l,\n" +
                "        partsupp ps,\n" +
                "        orders o,\n" +
                "        nation n\n" +
                "    where\n" +
                "        s.s_suppkey = l.l_suppkey\n" +
                "        and ps.ps_suppkey = l.l_suppkey\n" +
                "        and ps.ps_partkey = l.l_partkey\n" +
                "        and p.__key = l.l_partkey\n" +
                "        and o.o_orderkey = l.l_orderkey\n" +
                "        and s.s_nationkey = n.n_nationkey\n" +
                "        and p.p_name like ?\n" +
                "    ) as profit\n" +
                "group by\n" +
                "    nation,\n" +
                "    o_year\n" +
                "order by\n" +
                "    nation,\n" +
                "    o_year desc"
        , -1, color);
    }

    @Test
    public void testQ10() {
        LocalDate date = LocalDate.parse("1993-01-01");

        List<SqlRow> rows = execute(
            "select\n" +
                "    c.__key,\n" +
                "    c.c_name,\n" +
                "    sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,\n" +
                "    c.c_acctbal,\n" +
                "    n.n_name,\n" +
                "    c.c_address,\n" +
                "    c.c_phone,\n" +
                "    c.c_comment\n" +
                "from\n" +
                "    customer c,\n" +
                "    orders o,\n" +
                "    lineitem l,\n" +
                "    nation n\n" +
                "where\n" +
                "    c.__key = o.o_custkey\n" +
                "    and l.l_orderkey = o.o_orderkey\n" +
                "    and o.o_orderdate >= ?\n" +
                "    and o.o_orderdate < ?\n" +
                "    and l.l_returnflag = 'R'\n" +
                "    and c.c_nationkey = n.n_nationkey\n" +
                "group by\n" +
                "    c.__key,\n" +
                "    c.c_name,\n" +
                "    c.c_acctbal,\n" +
                "    c.c_phone,\n" +
                "    n.n_name,\n" +
                "    c.c_address,\n" +
                "    c.c_comment\n" +
                "order by\n" +
                "    revenue desc"
        , 20, date, date.plusMonths(3));
    }

    @Test
    public void testQ11() {
        String nation = "GERMANY";
        BigDecimal fraction = new BigDecimal("0.0001");

        List<SqlRow> rows = execute(
            "select\n" +
                "    ps.ps_partkey,\n" +
                "    sum(ps.ps_supplycost * ps.ps_availqty) as val\n" +
                "from\n" +
                "    partsupp ps,\n" +
                "    supplier s,\n" +
                "    nation n\n" +
                "where\n" +
                "    ps.ps_suppkey = s.s_suppkey\n" +
                "    and s.s_nationkey = n.n_nationkey\n" +
                "    and n.n_name = ?\n" +
                "group by\n" +
                "    ps.ps_partkey having\n" +
                "        sum(ps.ps_supplycost * ps.ps_availqty) > (\n" +
                "            select\n" +
                "                sum(ps2.ps_supplycost * ps2.ps_availqty) * ?\n" +
                "            from\n" +
                "                partsupp ps2,\n" +
                "                supplier s2,\n" +
                "                nation n2\n" +
                "            where\n" +
                "                ps2.ps_suppkey = s2.s_suppkey\n" +
                "                and s2.s_nationkey = n2.n_nationkey\n" +
                "                and n2.n_name = ?\n" +
                "        )\n" +
                "order by\n" +
                "    val desc"
        , -1, nation, fraction, nation);
    }

    @Test
    public void testQ12() {
        String shipmode1 = "MAIL";
        String shipmode2 = "SHIP";
        LocalDate date = LocalDate.parse("1994-01-01");

        List<SqlRow> rows = execute(
            "select\n" +
                "    l.l_shipmode,\n" +
                "    sum(case\n" +
                "        when o.o_orderpriority ='1-URGENT' or o.o_orderpriority ='2-HIGH'\n" +
                "        then 1\n" +
                "        else 0\n" +
                "    end) as high_line_count,\n" +
                "    sum(case\n" +
                "        when o.o_orderpriority <> '1-URGENT' and o.o_orderpriority <> '2-HIGH'\n" +
                "        then 1\n" +
                "        else 0\n" +
                "    end) as low_line_count\n" +
                "from\n" +
                "    orders o,\n" +
                "    lineitem l\n" +
                "where\n" +
                "    o.o_orderkey = l.l_orderkey\n" +
                "    and l.l_shipmode in (?, ?)\n" +
                "    and l.l_commitdate < l.l_receiptdate\n" +
                "    and l.l_shipdate < l.l_commitdate\n" +
                "    and l.l_receiptdate >= ?\n" +
                "    and l.l_receiptdate < ?\n" +
                "group by\n" +
                "    l.l_shipmode\n" +
                "order by\n" +
                "    l.l_shipmode"
        , -1, shipmode1, shipmode2, date, date.plusYears(1));
    }

    @Test
    public void testQ13() {
        String comment = "%special%requests%";

        List<SqlRow> rows = execute(
            "select\n" +
                "    c_count, count(*) as custdist\n" +
                "from (\n" +
                "    select\n" +
                "        c.__key,\n" +
                "        count(o.o_orderkey)\n" +
                "    from\n" +
                "        customer c left outer join orders o on\n" +
                "            c.__key = o.o_custkey\n" +
                "            and o.o_comment not like ?\n" +
                "    group by\n" +
                "        c.__key\n" +
                "    )as c_orders (__key, c_count)\n" +
                "group by\n" +
                "    c_count\n" +
                "order by\n" +
                "    custdist desc,\n" +
                "    c_count desc"
        , -1, comment);
    }

    @Test
    public void testQ14() {
        LocalDate date = LocalDate.parse("1995-09-01");

        List<SqlRow> rows = execute(
            "select\n" +
                "    100.00 * sum(case\n" +
                "        when p.p_type like 'PROMO%'\n" +
                "        then l.l_extendedprice*(1 - l.l_discount)\n" +
                "        else 0.0\n" +
                "    end) / sum(l.l_extendedprice * (1 - l.l_discount)) as promo_revenue\n" +
                "from\n" +
                "    lineitem l,\n" +
                "    part p\n" +
                "where\n" +
                "    l.l_partkey = p.__key\n" +
                "    and l.l_shipdate >= ?\n" +
                "    and l.l_shipdate < ?"
        , -1, date, date.plusMonths(1));
    }

    @Ignore
    @Test
    public void testQ15() {
        fail("Require views");
    }

    @Ignore
    @Test
    public void testQ16() {
        fail("Requires COUNT(DISTINCT) aggregate support");
    }

    @Test
    public void testQ17() {
        String brand = "Brand#23";
        String container = "MED BOX";

        List<SqlRow> rows = execute(
            "select\n" +
                "    sum(l.l_extendedprice) / 7.0 as avg_yearly\n" +
                "from\n" +
                "    lineitem l,\n" +
                "    part p\n" +
                "where\n" +
                "    p.__key = l.l_partkey\n" +
                "    and p.p_brand = ?\n" +
                "    and p.p_container = ?\n" +
                "    and l.l_quantity < (\n" +
                "        select\n" +
                "            0.2 * avg(l2.l_quantity)\n" +
                "        from\n" +
                "            lineitem l2\n" +
                "        where\n" +
                "            l2.l_partkey = p.__key\n" +
                ")"
        , -1, brand, container);
    }

    @Test
    public void testQ18() {
        int quantity = 300;

        List<SqlRow> rows = execute(
            "select\n" +
                "    c.c_name,\n" +
                "    c.__key,\n" +
                "    o.o_orderkey,\n" +
                "    o.o_orderdate,\n" +
                "    o.o_totalprice,\n" +
                "    sum(l.l_quantity)\n" +
                "from\n" +
                "    customer c,\n" +
                "    orders o,\n" +
                "    lineitem l\n" +
                "where\n" +
                "    o.o_orderkey in (\n" +
                "        select\n" +
                "            l2.l_orderkey\n" +
                "        from\n" +
                "            lineitem l2\n" +
                "        group by\n" +
                "            l2.l_orderkey having\n" +
                "                sum(l2.l_quantity) > ?\n" +
                "    )\n" +
                "    and c.__key = o.o_custkey\n" +
                "    and o.o_orderkey = l.l_orderkey\n" +
                "group by\n" +
                "    c.c_name,\n" +
                "    c.__key,\n" +
                "    o.o_orderkey,\n" +
                "    o.o_orderdate,\n" +
                "    o.o_totalprice\n" +
                "order by\n" +
                "    o.o_totalprice desc,\n" +
                "    o.o_orderdate"
        , 100, quantity);
    }

    @Ignore
    @Test
    public void testQ19() {
        int quantity1 = 1;
        int quantity2 = 10;
        int quantity3 = 20;
        String brand1 = "Brand#12";
        String brand2 = "Brand#23";
        String brand3 = "Brand#24";

        // TODO: We need "OR-to-UNION" rule here. Notice how conditions could easily be split into three normal equi-joins.
        //  But at the moment we fallback to cross-join with materialization which kills us.
        List<SqlRow> rows = execute(
            "select\n" +
                "    sum(l.l_extendedprice * (1 - l.l_discount) ) as revenue\n" +
                "from\n" +
                "    lineitem l,\n" +
                "    part p\n" +
                "where\n" +
                "    (\n" +
                "        p.__key = l.l_partkey\n" +
                "        and p.p_brand = ?\n" +
                "        and p.p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')\n" +
                "        and l.l_quantity >= ? and l.l_quantity <= ? + 10\n" +
                "        and p.p_size between 1 and 5\n" +
                "        and l.l_shipmode in ('AIR', 'AIR REG')\n" +
                "        and l.l_shipinstruct = 'DELIVER IN PERSON'\n" +
                "    )\n" +
                "    or\n" +
                "    (\n" +
                "        p.__key = l.l_partkey\n" +
                "        and p.p_brand = ?\n" +
                "        and p.p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')\n" +
                "        and l.l_quantity >= ? and l.l_quantity <= ? + 10\n" +
                "        and p.p_size between 1 and 10\n" +
                "        and l.l_shipmode in ('AIR', 'AIR REG')\n" +
                "        and l.l_shipinstruct = 'DELIVER IN PERSON'\n" +
                "    )\n" +
                "    or\n" +
                "    (\n" +
                "        p.__key = l.l_partkey\n" +
                "        and p.p_brand = ?\n" +
                "        and p.p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')\n" +
                "        and l.l_quantity >= ? and l.l_quantity <= ? + 10\n" +
                "        and p.p_size between 1 and 15\n" +
                "        and l.l_shipmode in ('AIR', 'AIR REG')\n" +
                "        and l.l_shipinstruct = 'DELIVER IN PERSON'\n" +
                "    )"
        , -1, brand1, quantity1, quantity1, brand2, quantity2, quantity2, brand3, quantity3, quantity3);
    }

    @Test
    public void testQ20() {
        String color = "forest%";
        LocalDate date = LocalDate.parse("1994-01-01");
        String nation = "CANADA";

        List<SqlRow> rows = execute(
            "select\n" +
                "    s.s_name,\n" +
                "    s.s_address\n" +
                "from\n" +
                "    supplier s, \n" +
                "    nation n\n" +
                "where\n" +
                "    s.s_suppkey in (\n" +
                "        select\n" +
                "            ps.ps_suppkey\n" +
                "        from\n" +
                "            partsupp ps\n" +
                "        where\n" +
                "            ps.ps_partkey in (\n" +
                "                select\n" +
                "                    p.__key\n" +
                "                from\n" +
                "                    part p\n" +
                "                where\n" +
                "                    p.p_name like ?\n" +
                "            )\n" +
                "            and ps.ps_availqty > (\n" +
                "                select\n" +
                "                    0.5 * sum(l.l_quantity)\n" +
                "                from\n" +
                "                    lineitem l\n" +
                "                where\n" +
                "                    l.l_partkey = ps.ps_partkey\n" +
                "                    and l.l_suppkey = ps.ps_suppkey\n" +
                "                    and l.l_shipdate >= ?\n" +
                "                    and l.l_shipdate < ?\n" +
                "            )\n" +
                "    )\n" +
                "    and s.s_nationkey = n.n_nationkey\n" +
                "    and n.n_name = ?\n" +
                "order by\n" +
                "    s.s_name"
        , -1, color, date, date.plusYears(1), nation);
    }

    @Test
    public void testQ21() {
        String nation = "SAUDI ARABIA";

        List<SqlRow> rows = execute(
            "select\n" +
                "    s.s_name,\n" +
                "    count(*) as numwait\n" +
                "from\n" +
                "    supplier s,\n" +
                "    lineitem l1,\n" +
                "    orders o,\n" +
                "    nation n\n" +
                "where\n" +
                "    s.s_suppkey = l1.l_suppkey\n" +
                "    and o.o_orderkey = l1.l_orderkey\n" +
                "    and o.o_orderstatus = 'F'\n" +
                "    and l1.l_receiptdate > l1.l_commitdate\n" +
                "    and exists (\n" +
                "        select\n" +
                "            *\n" +
                "        from\n" +
                "            lineitem l2\n" +
                "        where\n" +
                "            l2.l_orderkey = l1.l_orderkey\n" +
                "            and l2.l_suppkey <> l1.l_suppkey\n" +
                "    )\n" +
                "    and not exists (\n" +
                "        select\n" +
                "            *\n" +
                "\t\tfrom\n" +
                "            lineitem l3\n" +
                "        where\n" +
                "            l3.l_orderkey = l1.l_orderkey\n" +
                "            and l3.l_suppkey <> l1.l_suppkey\n" +
                "            and l3.l_receiptdate > l3.l_commitdate\n" +
                "    )\n" +
                "    and s.s_nationkey = n.n_nationkey\n" +
                "    and n.n_name = ?\n" +
                "group by\n" +
                "    s.s_name\n" +
                "order by\n" +
                "    numwait desc,\n" +
                "    s.s_name"
        , 100, nation);
    }

    @Test
    public void testQ22() {
        String i1 = "13";
        String i2 = "31";
        String i3 = "23";
        String i4 = "29";
        String i5 = "30";
        String i6 = "18";
        String i7 = "17";

        List<SqlRow> rows = execute(
            "select\n" +
                "    cntrycode,\n" +
                "    count(*) as numcust,\n" +
                "    sum(c_acctbal) as totacctbal\n" +
                "from (\n" +
                "    select\n" +
                "        substring(c.c_phone from 1 for 2) as cntrycode,\n" +
                "        c.c_acctbal\n" +
                "    from\n" +
                "        customer c\n" +
                "    where\n" +
                "        substring(c.c_phone from 1 for 2) in (?, ?, ?, ?, ?, ?, ?)\n" +
                "        and c.c_acctbal > (\n" +
                "            select\n" +
                "                avg(c2.c_acctbal)\n" +
                "            from\n" +
                "                customer c2\n" +
                "            where\n" +
                "                c2.c_acctbal > 0.00\n" +
                "                and substring (c2.c_phone from 1 for 2) in (?, ?, ?, ?, ?, ?, ?)\n" +
                "        )\n" +
                "        and not exists (\n" +
                "            select\n" +
                "                *\n" +
                "            from\n" +
                "                orders o\n" +
                "            where\n" +
                "                o.o_custkey = c.__key\n" +
                "        )\n" +
                "    ) as custsale\n" +
                "group by\n" +
                "    cntrycode\n" +
                "order by\n" +
                "    cntrycode"
        , -1, i1, i2, i3, i4, i5, i6, i7, i1, i2, i3, i4, i5, i6, i7);
    }

    private static List<SqlRow> execute(String sql, int rowCount, Object... args) {
        if (rowCount < 0) {
            rowCount = 100;
        }

        OptimizerContext.setOptimizerConfig(OptimizerConfig.builder().setStatisticsEnabled(true).build());

        SqlCursorImpl res = (SqlCursorImpl) member.getSqlService().query(sql, args);
        Plan plan = res.getPlan();

        System.out.println(">>> Explain:");
        for (Row explainRow : res.getPlan().getExplain().asRows()) {
            System.out.println("\t" + explainRow.get(0));
        }
        System.out.println();

        System.out.println("\n>>> Optimizer statistics (" + plan.getStatistics().getDuration() + " ms):");
        for (Map.Entry<String, Integer> entry : plan.getStatistics().getPhysicalRuleCalls().getRuleCalls().entrySet()) {
            System.out.println("\t" + entry.getKey() + " -> " + entry.getValue());
        }

        List<SqlRow> rows = new ArrayList<>();

        System.out.println("\n>>> Results: ");
        int cnt = 0;

        for (SqlRow row : res) {
            cnt++;

            if (cnt <= rowCount) {
                System.out.println("\t" + row);

                rows.add(row);
            }
        }

        System.out.println("\n>>> Total rows: " + cnt);
        System.out.println();

        assert cnt > 0;

        return rows;
    }
}
