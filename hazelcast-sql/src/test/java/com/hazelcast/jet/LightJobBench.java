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

package com.hazelcast.jet;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class LightJobBench extends JetTestSupport {

    private JetInstance inst;

    @Before
    public void before() {
        inst = createJetMember();
    }

    @Test
    public void jetBench() throws Exception {
        int warmUpIterations = 100;
        int realIterations = 2000;
        DAG dag = new DAG();
        dag.newVertex("v", Processors.noopP());
        logger.info("will submit " + warmUpIterations + " jobs");
        for (int i = 0; i < warmUpIterations; i++) {
            inst.newLightJob(dag).join();
        }
//        for (int i = 20; i >= 0; i--) {
//            System.out.println("attach profiler " + i);
//            Thread.sleep(1000);
//        }
        logger.info("warmup jobs done, starting benchmark");
        long start = System.nanoTime();
        for (int i = 0; i < realIterations; i++) {
            inst.newLightJob(dag).join();
        }
        long elapsedMicros = NANOSECONDS.toMicros(System.nanoTime() - start);
        System.out.println(realIterations + " jobs run in " + (elapsedMicros / realIterations) + " us/job");
    }

    @Test
    public void sqlBench() {
        int warmUpIterations = 100;
        int realIterations = 200;
        SqlService sqlService = inst.getSql();
        logger.info("will submit " + warmUpIterations + " jobs");
        inst.getMap("m").put(1, 1);
        int numRows = 0;
        for (int i = 0; i < warmUpIterations; i++) {
            for (SqlRow r : sqlService.execute("select * from m")) {
                numRows++;
            }
        }
        logger.info("warmup jobs done, starting benchmark");
        long start = System.nanoTime();
        for (int i = 0; i < realIterations; i++) {
            for (SqlRow r : sqlService.execute("select * from m")) {
                numRows++;
            }
        }
        long elapsedMicros = NANOSECONDS.toMicros(System.nanoTime() - start);
        System.out.println(numRows);
        System.out.println(realIterations + " queries run in " + (elapsedMicros / realIterations) + " us/job");
    }
}
