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

package com.hazelcast.sql;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.impl.SqlContext;
import com.hazelcast.sql.impl.SqlPrepare;
import com.hazelcast.sql.impl.SqlTable;
import com.hazelcast.sql.pojos.Person;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

public final class HazelcastSql {

    private final SqlPrepare sqlPrepare = new SqlPrepare();

    private final CalciteSchema schema = CalciteSchema.createRootSchema(true);

    private final JavaTypeFactory javaTypeFactory = new JavaTypeFactoryImpl();

    private HazelcastSql(HazelcastInstance instance) {
        // TODO: real schema support, where to get it?
        // TODO: make __key field to be a pseudo field
        schema.add("persons", new SqlTable(javaTypeFactory.createStructType(Person.class), instance.getMap("persons")));
    }

    public static HazelcastSql createFor(HazelcastInstance instance) {
        return new HazelcastSql(instance);
    }

    public Enumerable<Object> query(String query) throws Exception {
        SqlContext context = new SqlContext(javaTypeFactory, schema);

        SqlParser parser = SqlParser.create(query, SqlParser.configBuilder().build());

        SqlNode node = parser.parseQuery();

        JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RexBuilder rexBuilder = new RexBuilder(typeFactory);

        VolcanoPlanner planner = new VolcanoPlanner();

        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

//        HazelcastSqlValidator validator = new HazelcastSqlValidator(
//            SqlOperatorTable opTab,
//            SqlValidatorCatalogReader catalogReader,
//            typeFactory,
//            SqlConformanceEnum.DEFAULT
//        );

        SqlToRelConverter converter = new SqlToRelConverter(
            null, // Expander
            null, // Validator
            null, // Catalog
            cluster, // Cluster
            StandardConvertletTable.INSTANCE, // SqlRexConvertletTable
            SqlToRelConverter.configBuilder().withConvertTableAccess(false).build() // Config
        );

        SqlContext context2 = new SqlContext(javaTypeFactory, schema);

        CalcitePrepare.Dummy.push(context2);

        try {
            CalcitePrepare.ConvertResult convertResult = sqlPrepare.convert(context, query);

            RelRoot root = convertResult.root;

            System.out.println(root);

            HepProgramBuilder hepBuilder = new HepProgramBuilder();

            hepBuilder.addRuleInstance(ProjectFilterTransposeRule.INSTANCE);

            HepPlanner hepPlanner = new HepPlanner(
                hepBuilder.build()
            );

            hepPlanner.setRoot(root.rel);

            RelNode transformed = hepPlanner.findBestExp();

            System.out.println(transformed);
        }
        finally {
            CalcitePrepare.Dummy.pop(context2);
        }



//        RelRoot relNode = converter.convertQuery(node, false, false);

        CalcitePrepare.Dummy.push(context);
        try {
//        CalcitePrepare.ParseResult parseResult = sqlPrepare.parse(context, query);
//        System.out.println(parseResult.sqlNode);

//        CalcitePrepare.ConvertResult convertResult = sqlPrepare.convert(context, query);
//        System.out.println(convertResult.root);

            CalcitePrepare.CalciteSignature<Object> calciteSignature =
                    sqlPrepare.prepareSql(context, CalcitePrepare.Query.of(query), Object[].class, -1);
            context.setInternalParameters(calciteSignature.internalParameters);
            return calciteSignature.enumerable(context.getDataContext());
        } finally {
            CalcitePrepare.Dummy.pop(context);
        }
    }

    public String explain(String query) throws Exception {
        Enumerable<Object> result = query("explain plan including all attributes for " + query);
        Object first = result.first();
        return first instanceof Object[] ? (String) ((Object[]) first)[0] : (String) first;
    }

    public String explainLogical(String query) {
        SqlContext context = new SqlContext(javaTypeFactory, schema);

        CalcitePrepare.Dummy.push(context);
        try {
            CalcitePrepare.ConvertResult convertResult = sqlPrepare.convert(context, query);
            return RelOptUtil.dumpPlan("", convertResult.root.rel, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES);
        } finally {
            CalcitePrepare.Dummy.pop(context);
        }
    }

}
