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

package com.hazelcast.log;

import com.hazelcast.config.Config;
import com.hazelcast.config.LogConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.log.encoders.LongEncoder;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.io.Serializable;
import java.util.Optional;
import java.util.function.BinaryOperator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReduceTest extends HazelcastTestSupport {

    @Test
    public void reduce() {
        Config config = new Config();

        LogConfig logConfig = new LogConfig("foo")
                .setEncoder(new LongEncoder())
                .setSegmentSize(1024);
        config.addLogConfig(logConfig);

        HazelcastInstance hz = createHazelcastInstance(config);

        Log<Long> log = hz.getLog(logConfig.getName());
        long[] sequences = new long[16 * 1024];
        Long sum = 0l;
        for (int k = 0; k < sequences.length; k++) {
            Long item = new Long(k);
            sum += item;
            long sequence = log.put(1, item);
            sequences[k] = sequence;
        }

        Optional<Long> r = log.reduce(1, new SumAccumulator());
        assertTrue(r.isPresent());
        assertEquals(sum, r.get());
    }

    public static class SumAccumulator implements BinaryOperator<Long>, Serializable {
        @Override
        public Long apply(Long a1, Long a2) {
            return a1 + a2;
        }
    }
}
