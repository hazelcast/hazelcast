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

package com.hazelcast.jet.examples.imdg;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;

import java.util.Map.Entry;

import static com.hazelcast.function.Functions.entryKey;
import static com.hazelcast.jet.Util.entry;

/**
 * Demonstrates the usage of Hazelcast IMap as source and sink with the Pipeline API.
 */
public class MapSourceAndSinks {

    private static final int ITEM_COUNT = 10;

    private static final String MAP_SOURCE = "mapSource";
    private static final String MAP_SINK = "mapSink";

    private static final String MAP_WITH_MERGING_SOURCE = "mapWithMergingSource";
    private static final String MAP_WITH_MERGING_SINK = "mapWithMergingSink";

    private static final String MAP_WITH_UPDATING_SOURCE_SINK = "mapWithUpdatingSourceSink";

    private static final String MAP_WITH_ENTRYPROCESSOR_SOURCE_SINK = "mapWithEntryProcessorSourceSink";
    private final JetInstance jet;

    public MapSourceAndSinks(JetInstance jet) {
        this.jet = jet;
    }


    /**
     * This will take the contents of source map and writes it into the sink map.
     */
    private static Pipeline mapSourceAndSink(String sourceMapName, String sinkMapName) {
        Pipeline pipeline = Pipeline.create();

        pipeline.readFrom(Sources.map(sourceMapName))
                .writeTo(Sinks.map(sinkMapName));

        return pipeline;
    }

    /**
     * This will take the contents of a map and suffixes the value with
     * {@code odd} if the key is odd and with {@code even} if the key is even.
     */
    private static Pipeline mapWithUpdating(String mapName) {
        Pipeline pipeline = Pipeline.create();

        // Note that we use the same map for source and sink. The map reader
        // requires the map to not change while reading or it might produce
        // incorrect results. But our job does not add or remove any keys in
        // the map, it only changes the values, so we can use it. Note that
        // you can also use IMap.executeOnEntries() for this simple case,
        // but Jet pipeline has more features.
        pipeline.readFrom(Sources.<Integer, String>map(mapName))
                .writeTo(
                        Sinks.mapWithUpdating(
                                mapName,
                                (oldValue, item) ->
                                        item.getKey() % 2 == 0
                                                ? oldValue + "-even"
                                                : oldValue + "-odd"
                        )
                );

        return pipeline;
    }

    /**
     * This will take the contents of source map, maps all keys to a key called {@code sum }
     * and write it into sink map using an merge function which merges the map values by adding
     * old value and new value.
     */
    private static Pipeline mapWithMerging(String sourceMapName, String sinkMapName) {
        Pipeline pipeline = Pipeline.create();

        pipeline.readFrom(Sources.<Integer, Integer>map(sourceMapName))
                .map(e -> entry("sum", e.getValue()))
                .writeTo(
                        Sinks.mapWithMerging(
                                sinkMapName,
                                (oldValue, newValue) -> oldValue + newValue
                        )
                );
        return pipeline;
    }

    /**
     * This will take the contents of source map and apply entry processor to
     * increment the values by 5.
     */
    private static Pipeline mapWithEntryProcessor(String sourceMapName, String sinkMapName) {
        Pipeline pipeline = Pipeline.create();

        pipeline.readFrom(Sources.<Integer, Integer>map(sourceMapName))
                .writeTo(
                        Sinks.mapWithEntryProcessor(
                                sinkMapName,
                                entryKey(),
                                item -> new IncrementEntryProcessor(5)
                        )
                );

        return pipeline;
    }

    public static void main(String[] args) {
        JetInstance jet = Jet.newJetInstance();
        new MapSourceAndSinks(jet).go();
    }

    private void go() {
        try {

            System.out.println("----------Map Source and Sink ----------------");
            // insert sequence 0..9 into map as (0,0) , (1,1) .... ( 9,9)
            prepareSampleInput(jet, MAP_SOURCE);
            // execute the pipeline
            jet.newJob(mapSourceAndSink(MAP_SOURCE, MAP_SINK)).join();
            // print contents of the sink map
            dumpMap(jet, MAP_SINK);
            System.out.println("----------------------------------------------");


            System.out.println("--------------Map with Merging----------------");
            // insert sequence 0..9 into map as (0,0) , (1,1) .... ( 9,9)
            prepareSampleInput(jet, MAP_WITH_MERGING_SOURCE);
            // execute the pipeline
            jet.newJob(mapWithMerging(MAP_WITH_MERGING_SOURCE, MAP_WITH_MERGING_SINK)).join();
            // print contents of the sink map
            dumpMap(jet, MAP_WITH_MERGING_SINK);
            System.out.println("----------------------------------------------");


            System.out.println("------------Map with Updating ----------------");
            // insert sequence 0..9 into map as (0,"0") , (1,"1") .... ( 9,"9")
            prepareMapWithUpdatingSampleInput(jet, MAP_WITH_UPDATING_SOURCE_SINK);
            // execute the pipeline
            jet.newJob(mapWithUpdating(MAP_WITH_UPDATING_SOURCE_SINK)).join();
            // print contents of the sink map
            dumpMap(jet, MAP_WITH_UPDATING_SOURCE_SINK);
            System.out.println("----------------------------------------------");


            System.out.println("----------Map with EntryProcessor ------------");
            // insert sequence 0..9 into map as (0,0) , (1,1) .... ( 9,9)
            prepareSampleInput(jet, MAP_WITH_ENTRYPROCESSOR_SOURCE_SINK);
            // execute the pipeline
            jet.newJob(mapWithEntryProcessor(MAP_WITH_ENTRYPROCESSOR_SOURCE_SINK,
                    MAP_WITH_ENTRYPROCESSOR_SOURCE_SINK)).join();
            // print contents of the sink map
            dumpMap(jet, MAP_WITH_ENTRYPROCESSOR_SOURCE_SINK);
            System.out.println("----------------------------------------------");

        } finally {
            Jet.shutdownAll();

        }
    }


    /**
     * Dumps contents of the IMap named {@code mapName} to the output stream
     */
    private static void dumpMap(JetInstance instance, String mapName) {
        IMap<Object, Object> sinkMap = instance.getMap(mapName);
        System.out.println("Sink map size: " + sinkMap.size());
        System.out.println("Sink map entries: ");
        sinkMap.forEach((k, v) -> System.out.println(k + " - " + v));
    }

    /**
     * Inserts the sequence 0..ITEM_COUNT into {@code sourceMapName} as
     * (0,0) , (1,1) ....( ITEM_COUNT-1, ITEM_COUNT-1)
     */
    private static void prepareSampleInput(JetInstance instance, String sourceMapName) {
        IMap<Integer, Integer> sourceMap = instance.getMap(sourceMapName);
        for (int i = 0; i < ITEM_COUNT; i++) {
            sourceMap.put(i, i);
        }
    }

    /**
     * Inserts the sequence 0..ITEM_COUNT into {@code sourceMapName} as
     * (0,"0") , (1,"1") ....(ITEM_COUNT-1, "ITEM_COUNT-1")
     */
    private static void prepareMapWithUpdatingSampleInput(JetInstance instance, String sourceMapName) {
        IMap<Integer, String> sourceMap = instance.getMap(sourceMapName);
        for (int i = 0; i < ITEM_COUNT; i++) {
            sourceMap.put(i, Integer.toString(i));
        }
    }

    static class IncrementEntryProcessor implements EntryProcessor<Integer, Integer, Integer> {

        private int incrementBy;

        IncrementEntryProcessor(int incrementBy) {
            this.incrementBy = incrementBy;
        }

        @Override
        public Integer process(Entry<Integer, Integer> entry) {
            return entry.setValue(entry.getValue() + incrementBy);
        }
    }
}
