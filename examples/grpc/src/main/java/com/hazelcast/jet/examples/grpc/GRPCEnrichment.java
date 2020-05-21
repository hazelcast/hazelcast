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

package com.hazelcast.jet.examples.grpc;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.examples.grpc.datamodel.Broker;
import com.hazelcast.jet.examples.grpc.datamodel.Product;
import com.hazelcast.jet.examples.grpc.datamodel.Trade;
import com.hazelcast.jet.grpc.GrpcService;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamStage;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static com.hazelcast.function.Functions.entryValue;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.grpc.GrpcServices.bidirectionalStreamingService;
import static com.hazelcast.jet.grpc.GrpcServices.unaryService;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_CURRENT;
import static java.util.stream.Collectors.toMap;

/**
 * Demonstrates the usage of the Pipeline API to enrich a data stream. We
 * generate a stream of stock trade events and each event has an associated
 * product ID and broker ID. The reference lists of products and brokers
 * are stored in files. The goal is to enrich the trades with the actual
 * name of the products and the brokers.
 * <p>
 * We generate the stream of trade events by updating a single key in the
 * {@code trades} map which has the Event Journal enabled. The event
 * journal emits a stream of update events.
 */
public final class GRPCEnrichment {

    private static final String TRADES = "trades";
    private static final int PORT = 50051;

    private final JetInstance jet;

    private GRPCEnrichment(JetInstance jet) {
        this.jet = jet;
    }

    /**
     * Builds a pipeline which enriches the stream with the response from a
     * gRPC service.
     * <p>
     * It starts a gRPC server that will provide product and broker names based
     * on an ID. The job then enriches incoming trades using the service. This
     * sample demonstrates a way to call external service with an async API
     * using the {@link StreamStage#mapUsingServiceAsync}
     * method.
     */
    public static Pipeline enrichUsingGRPC() {
        // The stream to be enriched: trades
        Pipeline p = Pipeline.create();
        StreamStage<Trade> trades = p
                .readFrom(Sources.<Object, Trade>mapJournal(TRADES, START_FROM_CURRENT))
                .withoutTimestamps()
                .map(entryValue());

        ServiceFactory<?, ? extends GrpcService<ProductInfoRequest, ProductInfoReply>> productService = unaryService(
                () -> ManagedChannelBuilder.forAddress("localhost", PORT).useTransportSecurity().usePlaintext(),
                channel -> ProductServiceGrpc.newStub(channel)::productInfo
        );

        ServiceFactory<?, ? extends GrpcService<BrokerInfoRequest, BrokerInfoReply>> brokerService =
                bidirectionalStreamingService(
                        () -> ManagedChannelBuilder.forAddress("localhost", PORT).usePlaintext(),
                        channel -> BrokerServiceGrpc.newStub(channel)::brokerInfo
                );

        // Enrich the trade by querying the product and broker name from the gRPC services
        trades.mapUsingServiceAsync(productService,
                (service, trade) -> {
                    ProductInfoRequest request = ProductInfoRequest.newBuilder().setId(trade.productId()).build();
                    return service.call(request).thenApply(productReply -> tuple2(trade, productReply.getProductName()));
                })
              // input is (trade, product)
              .mapUsingServiceAsync(brokerService,
                      (service, t) -> {
                          BrokerInfoRequest request = BrokerInfoRequest.newBuilder().setId(t.f0().brokerId()).build();
                          return service.call(request)
                                        .thenApply(brokerReply -> tuple3(t.f0(), t.f1(), brokerReply.getBrokerName()));
                      })
              // output is (trade, productName, brokerName)
              .writeTo(Sinks.logger());
        return p;
    }

    public static void main(String[] args) throws Exception {
        JetInstance jet = Jet.bootstrappedInstance();

        new GRPCEnrichment(jet).go();
    }

    private void go() throws Exception {
        EventGenerator eventGenerator = new EventGenerator(jet.getMap(TRADES));
        eventGenerator.start();
        try (Closeable server = startGRPCServer()) {
            Pipeline p = enrichUsingGRPC();
            Job job = jet.newJob(p);
            eventGenerator.generateEventsForFiveSeconds();
            job.cancel();
            try {
                job.join();
            } catch (CancellationException ignored) {
            }
        } finally {
            eventGenerator.shutdown();
            Jet.shutdownAll();
        }
    }

    private static Closeable startGRPCServer() throws IOException {
        Map<Integer, Product> productMap = readLines("products.txt")
                .collect(toMap(Entry::getKey, e -> new Product(e.getKey(), e.getValue())));
        Map<Integer, Broker> brokerMap = readLines("brokers.txt")
                .collect(toMap(Entry::getKey, e -> new Broker(e.getKey(), e.getValue())));

        ExecutorService executor = Executors.newFixedThreadPool(4);
        Server server = ServerBuilder.forPort(PORT)
                                     .executor(executor)
                                     .addService(new ProductServiceImpl(productMap))
                                     .addService(new BrokerServiceImpl(brokerMap))
                                     .build()
                                     .start();
        System.out.println("*** Server started, listening on " + PORT);

        return () -> {
            server.shutdown();
            executor.shutdown();
        };
    }

    private static Stream<Map.Entry<Integer, String>> readLines(String file) {
        try {
            InputStream stream = GRPCEnrichment.class.getResourceAsStream("/" + file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
            return reader.lines().map(GRPCEnrichment::splitLine);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Map.Entry<Integer, String> splitLine(String e) {
        int commaPos = e.indexOf(',');
        return entry(Integer.valueOf(e.substring(0, commaPos)), e.substring(commaPos + 1));
    }
}
