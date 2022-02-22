/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.aggregation;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.internal.util.UuidUtil;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import static java.util.Collections.singletonList;

/**
 * This is no JUnit test.
 */
public class MapWordCountAggregationBenchmark extends HazelcastTestSupport {

    private static final String[] DATA_RESOURCES_TO_LOAD = {"dracula.txt"};

    private static final String MAP_NAME = "articles";

    public static void main(String[] args) throws Exception {
        HazelcastInstance hazelcastInstance = buildCluster(3);

        try {
            System.out.println("Filling map...");
            for (int i = 0; i < 20 * 8; i++) {
                //fillMapWithDataEachLineNewEntry(hazelcastInstance);
                fillMapWithData(hazelcastInstance);
            }
            IMap<String, String> map = hazelcastInstance.getMap(MAP_NAME);

            System.out.println("Garbage collecting...");
            for (int i = 0; i < 10; i++) {
                System.gc();
            }

            for (int i = 0; i < 10; i++) {
                System.out.println("Executing job...");
                long start = System.currentTimeMillis();

                Map<String, MutableInt> result = map.aggregate(new WordCountAggregator());

                System.err.println(result.size());
                System.err.println("TimeTaken=" + (System.currentTimeMillis() - start));
                System.err.println("---------------------------------------------");
                System.gc();
            }
        } finally {
            Hazelcast.shutdownAll();
        }
    }

    private static HazelcastInstance buildCluster(int memberCount) {
        Config config = new Config();
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(true);
        networkConfig.getJoin().getTcpIpConfig().setMembers(singletonList("127.0.0.1"));

        MapConfig mapConfig = new MapConfig();
        mapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        mapConfig.setName(MAP_NAME);
        mapConfig.setBackupCount(0);
        config.addMapConfig(mapConfig);

        config.setProperty("hazelcast.query.predicate.parallel.evaluation", "true");
        config.setProperty("hazelcast.aggregation.accumulation.parallel.evaluation", "true");

        HazelcastInstance[] hazelcastInstances = new HazelcastInstance[memberCount];
        for (int i = 0; i < memberCount; i++) {
            hazelcastInstances[i] = Hazelcast.newHazelcastInstance(config);
        }
        return hazelcastInstances[0];
    }

    private static void fillMapWithData(HazelcastInstance hazelcastInstance) throws Exception {

        IMap<String, String> map = hazelcastInstance.getMap(MAP_NAME);
        for (String file : DATA_RESOURCES_TO_LOAD) {
            InputStream is = MapWordCountAggregationBenchmark.class.getResourceAsStream("/wordcount/" + file);
            LineNumberReader reader = new LineNumberReader(new InputStreamReader(is));

            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
            map.put(UuidUtil.newSecureUuidString(), sb.toString());

            is.close();
            reader.close();
        }
    }

    private static void fillMapWithDataEachLineNewEntry(HazelcastInstance hazelcastInstance) throws Exception {

        IMap<String, String> map = hazelcastInstance.getMap(MAP_NAME);
        for (String file : DATA_RESOURCES_TO_LOAD) {
            InputStream is = MapWordCountAggregationBenchmark.class.getResourceAsStream("/wordcount/" + file);
            LineNumberReader reader = new LineNumberReader(new InputStreamReader(is));

            int batchSize = 10000;
            int batchSizeCount = 0;
            Map<String, String> batch = new HashMap<String, String>(batchSize);
            String line;
            while ((line = reader.readLine()) != null) {
                batch.put(UuidUtil.newSecureUuidString(), line);
                batchSizeCount++;
                if (batchSizeCount == batchSize) {
                    map.putAll(batch);
                    batchSizeCount = 0;
                    batch.clear();
                }
            }

            if (batchSizeCount > 0) {
                map.putAll(batch);
                batch.clear();
            }

            is.close();
            reader.close();
        }
    }

    private static class MutableInt implements Serializable {

        private int value = 0;

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }

    private static String cleanWord(String word) {
        return word.replaceAll("[^A-Za-z0-9]", "");
    }

    private static class WordCountAggregator implements Aggregator<Map.Entry<String, String>, Map<String, MutableInt>> {

        Map<String, MutableInt> result = new HashMap<String, MutableInt>(1000);

        void accumulate(String value, int times) {
            StringTokenizer tokenizer = new StringTokenizer(value);

            while (tokenizer.hasMoreTokens()) {
                String word = cleanWord(tokenizer.nextToken()).toLowerCase(StringUtil.LOCALE_INTERNAL);

                MutableInt count = result.get(word);
                if (count == null) {
                    count = new MutableInt();
                    result.put(word, count);
                }
                count.value += times;
            }
        }

        @Override
        public void accumulate(Map.Entry<String, String> entry) {
            accumulate(entry.getValue(), 1);
        }

        @Override
        public void combine(Aggregator aggregator) {
            WordCountAggregator aggr = (WordCountAggregator) aggregator;
            for (Map.Entry<String, MutableInt> toCombine : aggr.result.entrySet()) {
                doCombine(toCombine);
            }
        }

        private void doCombine(Map.Entry<String, MutableInt> toCombine) {
            String word = toCombine.getKey();
            MutableInt count = result.get(word);
            if (count == null) {
                count = new MutableInt();
                result.put(word, count);
            }
            count.value += toCombine.getValue().value;
        }

        @Override
        public Map<String, MutableInt> aggregate() {
            return result;
        }
    }
}
