package com.hazelcast.aggregation;

import com.hazelcast.aggregation.impl.AbstractAggregator;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.util.UuidUtil;
import org.junit.Ignore;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

@Ignore("Run manually only")
public class MapWordCountAggregationPerformanceTest extends HazelcastTestSupport {

    private static final String[] DATA_RESOURCES_TO_LOAD = {"dracula.txt"};

    private static final String MAP_NAME = "articles";

    public static void main(String[] args)
            throws Exception {

        // Prepare Hazelcast cluster
        HazelcastInstance hazelcastInstance = buildCluster(3);

        try {
            // Read data
            System.out.println("Filling map...");
            for (int i = 0; i < 20 * 8; i++) {
                //  fillMapWithDataEachLineNewEntry(hazelcastInstance);
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
            // Shutdown cluster
            Hazelcast.shutdownAll();
        }
    }

    public static HazelcastInstance buildCluster(int memberCount) {
        Config config = new Config();
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(true);
        networkConfig.getJoin().getTcpIpConfig().setMembers(Arrays.asList(new String[]{"127.0.0.1"}));

        MapConfig mapConfig = new MapConfig();
        mapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        mapConfig.setName(MAP_NAME);
        mapConfig.setBackupCount(0);
        config.addMapConfig(mapConfig);

        config.setProperty("hazelcast.query.predicate.parallel.evaluation", "true");
        config.setProperty("hazelcast.aggregation.accumulation.parallel.evaluation", "true");
        config.setProperty("hazelcast.logging.type", "log4j");


        HazelcastInstance[] hazelcastInstances = new HazelcastInstance[memberCount];
        for (int i = 0; i < memberCount; i++) {
            hazelcastInstances[i] = Hazelcast.newHazelcastInstance(config);
        }
        return hazelcastInstances[0];
    }

    private static void fillMapWithData(HazelcastInstance hazelcastInstance)
            throws Exception {

        IMap<String, String> map = hazelcastInstance.getMap(MAP_NAME);
        for (String file : DATA_RESOURCES_TO_LOAD) {
            InputStream is = MapWordCountAggregationPerformanceTest.class.getResourceAsStream("/wordcount/" + file);
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

    private static void fillMapWithDataEachLineNewEntry(HazelcastInstance hazelcastInstance)
            throws Exception {

        IMap<String, String> map = hazelcastInstance.getMap(MAP_NAME);
        for (String file : DATA_RESOURCES_TO_LOAD) {
            InputStream is = MapWordCountAggregationPerformanceTest.class.getResourceAsStream("/wordcount/" + file);
            LineNumberReader reader = new LineNumberReader(new InputStreamReader(is));

            int batchSize = 10000;
            int batchSizeCount = 0;
            Map batch = new HashMap(batchSize);
            String line = null;
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

    public static String cleanWord(String word) {
        return word.replaceAll("[^A-Za-z0-9]", "");
    }

    private static class WordCountAggregator extends AbstractAggregator<Map<String, MutableInt>, String, String> {

        Map<String, MutableInt> result = new HashMap<String, MutableInt>(1000);

        public void accumulate(String value, int times) {
            StringTokenizer tokenizer = new StringTokenizer(value);

            while (tokenizer.hasMoreTokens()) {
                String word = cleanWord(tokenizer.nextToken()).toLowerCase();

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

        public void doCombine(Map.Entry<String, MutableInt> toCombine) {
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
