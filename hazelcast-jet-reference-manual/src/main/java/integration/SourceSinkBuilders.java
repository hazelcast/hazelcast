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

package integration;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.SourceBuilder.SourceBuffer;
import com.hazelcast.jet.pipeline.StreamSource;
import com.hazelcast.jet.pipeline.StreamSourceStage;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Writer;

import static com.hazelcast.jet.pipeline.SinkBuilder.sinkBuilder;
import static com.hazelcast.jet.pipeline.Sources.list;

public class SourceSinkBuilders {
    static void s1() {
        //tag::s1[]
        BatchSource<String> fileSource = SourceBuilder
            .batch("file-source", x ->                               //<1>
                new BufferedReader(new FileReader("input.txt")))
            .<String>fillBufferFn((in, buf) -> {                     //<2>
                String line = in.readLine();
                if (line != null) {
                    buf.add(line);
                } else {
                    buf.close();                                     //<3>
                }
            })
            .destroyFn(BufferedReader::close)
            .build();
        Pipeline p = Pipeline.create();
        BatchStage<String> srcStage = p.drawFrom(fileSource);
        //end::s1[]
    }

    static void s1a() {
        //tag::s1a[]
        BatchSource<String> fileSource = SourceBuilder
            .batch("file-source", x ->
                new BufferedReader(new FileReader("input.txt")))
            .<String>fillBufferFn((in, buf) -> {
                for (int i = 0; i < 128; i++) {
                    String line = in.readLine();
                    if (line == null) {
                        buf.close();
                        return;
                    }
                    buf.add(line);
                }
            })
            .destroyFn(BufferedReader::close)
            .build();
        //end::s1a[]
    }

    static void s2() {
        //tag::s2[]
        StreamSource<String> httpSource = SourceBuilder
            .stream("http-source", ctx -> HttpClients.createDefault())
            .<String>fillBufferFn((httpc, buf) -> {
                HttpGet request = new HttpGet("localhost:8008");
                InputStream content = httpc.execute(request)
                                           .getEntity()
                                           .getContent();
                BufferedReader reader = new BufferedReader(
                    new InputStreamReader(content)
                );
                reader.lines().forEach(buf::add);
            })
            .destroyFn(CloseableHttpClient::close)
            .build();
        Pipeline p = Pipeline.create();
        StreamSourceStage<String> srcStage = p.drawFrom(httpSource);
        //end::s2[]
    }


    static void s2a() {
        //tag::s2a[]
        StreamSource<String> httpSource = SourceBuilder
            .timestampedStream("http-source", ctx -> HttpClients.createDefault())
            .<String>fillBufferFn((httpc, buf) -> {
                HttpGet request = new HttpGet("localhost:8008");
                InputStream content = httpc.execute(request)
                                           .getEntity()
                                           .getContent();
                BufferedReader reader = new BufferedReader(
                    new InputStreamReader(content)
                );
                reader.lines().forEach(item -> {
                    long timestamp = Long.valueOf(item.substring(0, 9));
                    buf.add(item.substring(9), timestamp);
                });
            })
            .destroyFn(CloseableHttpClient::close)
            .build();
        //end::s2a[]
    }

    static void s3() {
        //tag::s3[]
        class SourceContext {
            private final int limit;
            private final int step;
            private int currentValue;

            SourceContext(Processor.Context ctx, int limit) {
                this.limit = limit;
                this.step = ctx.totalParallelism();
                this.currentValue = ctx.globalProcessorIndex();
            }

            void addToBuffer(SourceBuffer<Integer> buffer) {
                if (currentValue < limit) {
                    buffer.add(currentValue);
                    currentValue += step;
                } else {
                    buffer.close();
                }
            }
        }

        BatchSource<Integer> sequenceSource = SourceBuilder
            .batch("seq-source", procCtx -> new SourceContext(procCtx, 1_000))
            .fillBufferFn(SourceContext::addToBuffer)
            .distributed(2)  //<1>
            .build();
        //end::s3[]
    }

    static void s3a() {
        //tag::s3a[]
        StreamSource<Integer> faultTolerantSource = SourceBuilder
            .stream("fault-tolerant-source", processorContext -> new int[1])
            .<Integer>fillBufferFn((numToEmit, buffer) ->
                buffer.add(numToEmit[0]++))
            .createSnapshotFn(numToEmit -> numToEmit[0])                //<1>
            .restoreSnapshotFn(
                (numToEmit, saved) -> numToEmit[0] = saved.get(0))  //<2>
            .build();
        //end::s3a[]
    }

    static void s4() {
        //tag::s4[]
        Sink<Object> sink = sinkBuilder(
            "file-sink", x -> new PrintWriter(new FileWriter("output.txt")))
            .receiveFn((out, item) -> out.println(item.toString()))
            .destroyFn(PrintWriter::close)
            .build();
        Pipeline p = Pipeline.create();
        p.drawFrom(list("input"))
         .drainTo(sink);
        //end::s4[]
    }

    static void s5() {
        //tag::s5[]
        Sink<Object> sink = sinkBuilder("file-sink", x -> new StringBuilder())
            .receiveFn((buf, item) -> buf.append(item).append('\n'))
            .flushFn(buf -> {
                try (Writer out = new FileWriter("output.txt", true)) {
                    out.write(buf.toString());
                    buf.setLength(0);
                }
            })
            .build();
        //end::s5[]
    }

    static void s6() {
        //tag::s6[]
        //end::s6[]
    }

    static void s7() {
        //tag::s7[]
        //end::s7[]
    }

    static void s8() {
        //tag::s8[]
        //end::s8[]
    }

}
