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

package com.hazelcast.jet.examples.python;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Observable;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.python.PythonServiceConfig;

import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;

import static com.hazelcast.jet.python.PythonTransforms.mapUsingPython;

/**
 * This example shows you how to invoke a Python function to process the
 * data in the Jet pipeline. The function gets a batch of items in a list
 * and must return a list of result items.
 * <p>
 * The provided code uses the NumPy library to transform the input list by
 * taking the square root of each element. It uses {@code
 * src/main/resources/python} in this project as the Python project
 * directory. There are two files there: {@code requirements.txt} that
 * declares NumPy as a dependency and {@code take_sqrt.py} that defines the
 * {@code transform_list} function that Jet will call with the pipeline
 * data.
 */
public class Python {

    private static final String RESULTS = "python_results";

    private static Pipeline buildPipeline(String baseDir) {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.itemStream(10, (ts, seq) -> bigRandomNumberAsString()))
         .withoutTimestamps()
         .apply(mapUsingPython(new PythonServiceConfig()
                 .setBaseDir(baseDir)
                 .setHandlerModule("take_sqrt")))
         .setLocalParallelism(1) // controls how many Python processes will be used
         .writeTo(Sinks.observable(RESULTS));
        return p;
    }

    private static String bigRandomNumberAsString() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        return "" + rnd.nextLong(Long.MAX_VALUE) + rnd.nextLong(Long.MAX_VALUE)
                + rnd.nextLong(Long.MAX_VALUE) + rnd.nextLong(Long.MAX_VALUE);
    }

    public static void main(String[] args) {
        Path baseDir = Util.getFilePathOfClasspathResource("python");
        Pipeline p = buildPipeline(baseDir.toString());

        JetInstance jet = Jet.bootstrappedInstance();
        try {
            Observable<String> observable = jet.getObservable(RESULTS);
            observable.addObserver(System.out::println);
            JobConfig config = new JobConfig().setName("python-mapping");
            jet.newJobIfAbsent(p, config).join();
        } finally {
            Jet.shutdownAll();
        }
    }
}
