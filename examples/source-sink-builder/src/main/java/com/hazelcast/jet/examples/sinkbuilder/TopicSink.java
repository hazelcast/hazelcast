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

package com.hazelcast.jet.examples.sinkbuilder;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.topic.ITopic;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.function.Consumer;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.pipeline.SinkBuilder.sinkBuilder;

/**
 * Shows how to use the {@link com.hazelcast.jet.pipeline.SinkBuilder} to
 * build a sink for the Jet pipeline. The pipeline reads text files, finds
 * the lines starting with "the" and publishes them to a Hazelcast {@code
 * ITopic}. The sample code subscribes itself to the {@code ITopic} and
 * prints the job's output.
 */
public class TopicSink {

    private static final String TOPIC_NAME = "topic";

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.files(getBooksPath()))
         .filter(line -> line.startsWith("The "))
         .writeTo(buildTopicSink());
        return p;
    }

    private static Sink<String> buildTopicSink() {
        return sinkBuilder("topicSink(" + TOPIC_NAME + ')',
                        jet -> jet.jetInstance().getHazelcastInstance().<String>getTopic(TOPIC_NAME))
                .<String>receiveFn((topic, message) -> topic.publish(message))
                .build();
    }

    /**
     * Creates a Hazelcast Jet cluster, attaches a topic listener and runs the pipeline
     */
    public static void main(String[] args) {
        try {
            JetInstance jet = Jet.bootstrappedInstance();
            System.out.println("Configure Topic Listener");
            ITopic<String> topic = jet.getHazelcastInstance().getTopic(TOPIC_NAME);
            addListener(topic, e -> System.out.println("Line starts with `The`: " + e));

            System.out.println("\nRunning the pipeline");
            Pipeline p = buildPipeline();
            jet.newJob(p).join();
        } finally {
            Jet.shutdownAll();
        }
    }

    /**
     * Returns the path of the books which will feed the pipeline
     */
    private static String getBooksPath() {
        try {
            return Paths.get(TopicSink.class.getResource("/books").toURI()).toString();
        } catch (URISyntaxException e) {
            throw rethrow(e);
        }
    }

    /**
     * Attaches a listener to {@link ITopic} which passes published items to the specified consumer
     *
     * @param topic    topic instance which the listener will be added
     * @param consumer message consumer that the added items will be passed on.
     */
    private static void addListener(ITopic<String> topic, Consumer<String> consumer) {
        topic.addMessageListener(event -> consumer.accept(event.getMessageObject()));
    }

}
