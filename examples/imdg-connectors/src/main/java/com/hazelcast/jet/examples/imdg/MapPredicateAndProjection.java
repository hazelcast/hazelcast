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

import com.hazelcast.collection.IList;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.Predicates;

import java.io.IOException;

/**
 * Demonstrates the usage of Hazelcast IMap as source and using
 * Hazelcast predicates and projections on Hazelcast portable types.
 * <p>
 * By using a portable type along with predicates and projections, we avoid
 * the need for deserializing the whole object. For more information,
 * please refer to the Hazelcast Reference Manual, sections "Implementing
 * Portable Serialization" and "Distributed Query".
 */
public class MapPredicateAndProjection {

    private static final int ITEM_COUNT = 100_000;
    private static final String SOURCE_NAME = "source";
    private static final String SINK_NAME = "sink";

    public static void main(String[] args) {
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().getSerializationConfig().addPortableFactory(
                TradePortableFactory.FACTORY_ID, new TradePortableFactory()
        );

        JetInstance jet = Jet.newJetInstance(config);
        try {
            System.out.println("Creating " + ITEM_COUNT + " entries...");
            IMap<String, Trade> sourceMap = jet.getMap(SOURCE_NAME);
            for (int i = 0; i < ITEM_COUNT; i++) {
                String ticker = "ticker-" + i;
                sourceMap.put(ticker, new Trade(ticker, i));
            }

            // by using named attributes for predicate and projection,
            // we can avoid the need for deserializing the whole object.
            Pipeline p1 = Pipeline.create();
            p1.<String>readFrom(Sources.map(
                    SOURCE_NAME,
                    Predicates.lessThan("price", 10),
                    Projections.singleAttribute("ticker"))
            ).writeTo(Sinks.list(SINK_NAME));
            System.out.println("\n\nExecuting job 1...\n");
            jet.newJob(p1).join();

            IList<String> sink = jet.getList(SINK_NAME);
            System.out.println("Sink items using predicates and projections: " + sink.subList(0, sink.size()));
            sink.clear();

            // using lambdas the whole object will need to be deserialized,
            // but the predicate and projection will still be applied at the
            // source
            Pipeline p2 = Pipeline.create();
            p2.readFrom(Sources.<String, String, Trade>map(
                    SOURCE_NAME,
                    e -> e.getValue().getPrice() < 10,
                    e -> e.getValue().getTicker())
            ).writeTo(Sinks.list(SINK_NAME));
            System.out.println("\n\nExecuting job 2...\n");
            jet.newJob(p2).join();

            System.out.println("Sink items using lambdas: " + sink.subList(0, sink.size()));

        } finally {
            Jet.shutdownAll();
        }
    }

    static class Trade implements Portable {

        static final int CLASS_ID = 1;

        private String ticker;
        private int price;

        private Trade() {
        }

        Trade(String ticker, int price) {
            this.ticker = ticker;
            this.price = price;
        }

        public String getTicker() {
            return ticker;
        }

        public void setTicker(String ticker) {
            this.ticker = ticker;
        }

        public int getPrice() {
            return price;
        }

        public void setPrice(int price) {
            this.price = price;
        }

        @Override
        public int getFactoryId() {
            return TradePortableFactory.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return CLASS_ID;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeUTF("ticker", ticker);
            writer.writeInt("price", price);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            ticker = reader.readUTF("ticker");
            price = reader.readInt("price");
        }

        @Override public String toString() {
            return "Trade{" +
                    "ticker='" + ticker + '\'' +
                    ", price=" + price +
                    '}';
        }
    }

    static class TradePortableFactory implements PortableFactory {
        static final int FACTORY_ID = 1;

        @Override
        public Portable create(int classId) {
            if (classId == Trade.CLASS_ID) {
                return new Trade();
            }
            throw new UnsupportedOperationException();
        }
    }
}
