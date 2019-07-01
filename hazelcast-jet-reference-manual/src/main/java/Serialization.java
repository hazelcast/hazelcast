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

import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;

import java.util.ArrayList;
import java.util.List;


public class Serialization {

    void splitAndMutate() {
        //tag::split-and-mutate[]
        class Person {
            private String name;

            String name() {
                return name;
            }

            Person addToName(String suffix) {
                name += suffix;
                return this;
            }
        }

        //end::split-and-mutate[]
        BatchSource<Person> personSource = null;
        //tag::split-and-mutate[]
        Pipeline p = Pipeline.create();
        BatchStage<Person> sourceStage = p.drawFrom(personSource);
        BatchStage<String> names = sourceStage
                .map(person -> person.name()); // <1>
        // don't do this!
        BatchStage<Person> juniors = sourceStage
                .map(person -> person.addToName(" Jr.")); // <2>
        //end::split-and-mutate[]
    }

    void modifyEmitted() {
        BatchSource<String> source = null;

        //tag::modify-emitted[]
        Pipeline p = Pipeline.create();
        ContextFactory<List<String>> contextFactory =
                ContextFactory.withCreateFn(procCtx -> new ArrayList<>());
        p.drawFrom(source)
         .mapUsingContext(contextFactory, (list, item) -> {
             // Don't do this!
             list.add(item);
             return list; // <1>
         });
        //end::modify-emitted[]
    }
}
