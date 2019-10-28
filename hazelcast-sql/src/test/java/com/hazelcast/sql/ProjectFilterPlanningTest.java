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
import com.hazelcast.sql.impl.QueryFragment;
import com.hazelcast.sql.impl.expression.CallOperator;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import com.hazelcast.sql.impl.expression.string.StringFunction;
import com.hazelcast.sql.support.ModelGenerator;
import com.hazelcast.sql.support.SqlTestSupport;
import com.hazelcast.sql.support.plan.PhysicalPlanChecker;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ProjectFilterPlanningTest extends SqlTestSupport {
    private static TestHazelcastInstanceFactory factory;
    private static HazelcastInstance member;

    @BeforeClass
    public static void beforeClass() {
        factory = new TestHazelcastInstanceFactory(2);

        member = factory.newHazelcastInstance();
        factory.newHazelcastInstance();

        ModelGenerator.generatePerson(member);
    }

    @AfterClass
    public static void afterClass() {
        if (factory != null) {
            factory.shutdownAll();
        }
    }

    /**
     * Make sure that Project <- Filter <- Scan gets merged into Scan
     */
    @Test
    public void testProjectFilterScanMerge() {
        QueryFragment fragment = getSingleFragmentFromPlan(
            member,
            "SELECT c.name FROM city c WHERE c.__key = 1"
        );

        Expression<Boolean> filter = new ComparisonPredicate(
            new ColumnExpression(0),
            new ConstantExpression<>(1),
            CallOperator.EQUALS
        );

        assertFragment(fragment,
            PhysicalPlanChecker.newBuilder()
                .addReplicatedMapScan("city", asList("__key", "name"), asList(1), filter)
                .addRoot()
                .build()
        );
    }

    /**
     * Make sure that complex project is not merged into scan.
     */
    @Test
    public void testProjectWithExpressionNoMerge() {
        QueryFragment fragment = getSingleFragmentFromPlan(
            member,
            "SELECT LENGTH(c.name) FROM city c"
        );

        assertFragment(fragment,
            PhysicalPlanChecker.newBuilder()
                .addReplicatedMapScan("city", asList("name"), asList(0), null)
                .addProject(asList(new StringFunction(new ColumnExpression(0), CallOperator.CHAR_LENGTH)))
                .addRoot()
                .build()
        );
    }

    @Test(timeout = Long.MAX_VALUE)
    public void testProjectScanMerge() {
        QueryFragment fragment = getSingleFragmentFromPlan(
            member,
            "SELECT c.name FROM city c"
        );

        assertFragment(
            fragment,
            PhysicalPlanChecker.newBuilder()
                .addReplicatedMapScan(ModelGenerator.CITY, asList("name"), asList(0), null)
                .addRoot()
                .build()
        );
    }

    @Test
    public void testProjectProjectScanMerge() {
        QueryFragment fragment = getSingleFragmentFromPlan(
            member,
            "SELECT name FROM (SELECT c.__key, c.name FROM city c)"
        );

        assertFragment(
            fragment,
            PhysicalPlanChecker.newBuilder()
                .addReplicatedMapScan(ModelGenerator.CITY, asList("__key", "name"), asList(1), null)
                .addRoot()
                .build()
        );
    }
}
