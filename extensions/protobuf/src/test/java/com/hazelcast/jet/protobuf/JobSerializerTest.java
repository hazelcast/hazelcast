/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.protobuf;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.proxy.ClientListProxy;
import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.test.AssertionSinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.protobuf.Messages.Animal;
import com.hazelcast.jet.protobuf.Messages.Person;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.stream.IntStream;

import static com.hazelcast.function.FunctionEx.identity;
import static com.hazelcast.jet.pipeline.ServiceFactories.sharedService;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Category({QuickTest.class, ParallelJVMTest.class})
public class JobSerializerTest extends SimpleTestInClusterSupport {

    private static final int PERSON_TYPE_ID = 1;
    private static final int ANIMAL_TYPE_ID = 2;

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig()
                    .addSerializerConfig(
                            new SerializerConfig().setTypeClass(Person.class).setClass(PersonSerializer.class)
                    );

        initializeWithClient(2, config, clientConfig);
    }

    @Test
    public void when_serializerIsNotRegistered_then_throwsException() {
        String listName = "list-1";
        List<Person> list = client().getList(listName);
        list.add(Person.newBuilder().setName("Joe").setAge(33).build());

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(Sources.<Person>list(listName))
                .map(Person::getName)
                .writeTo(Sinks.logger());

        assertThatThrownBy(() -> client().getJet().newJob(pipeline, new JobConfig()).join())
                .hasCauseInstanceOf(JetException.class);
    }

    @Test
    public void when_serializerIsRegistered_then_itIsAvailableForTheJob() {
        String listName = "list-2";
        List<Person> list = client().getList(listName);
        list.add(Person.newBuilder().setName("Joe").setAge(33).build());

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(Sources.<Person>list(listName))
                .map(Person::getName)
                .writeTo(AssertionSinks.assertAnyOrder(singletonList("Joe")));

        client().getJet().newJob(
                pipeline,
                new JobConfig().registerSerializer(Person.class, PersonSerializer.class)
        ).join();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void when_serializerIsRegisteredWithTheHook_then_itIsAvailableForTheJob() {
        String listName = "list-3";

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items("Mustang"))
                .map(name -> Animal.newBuilder().setName(name).build())
                .writeTo(Sinks.list(listName));

        client().getJet().newJob(pipeline, new JobConfig()).join();

        // Protocol Buffer types implement Serializable, to make sure hook registered serializer is used we check the
        // type id
        ClientListProxy<Animal> proxy = ((ClientListProxy) client().getList(listName));
        assertThat(proxy.dataSubList(0, 1).get(0).getType()).isEqualTo(ANIMAL_TYPE_ID);
    }

    @Test
    public void when_serializerIsRegisteredForDistributedJob_then_itIsAvailableForAllStages() {
        List<String> input = IntStream.range(0, 10_000).boxed().map(t -> Integer.toString(t)).collect(toList());

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.items(input))
                .map(name -> Person.newBuilder().setName(name).build())
                .groupingKey(identity())
                .filterUsingService(sharedService(ctx -> null), (s, k, v) -> true)
                .map(person -> person.getName())
                .writeTo(AssertionSinks.assertAnyOrder(input));

        client().getJet().newJob(
                pipeline,
                new JobConfig().registerSerializer(Person.class, PersonSerializer.class)
        ).join();
    }

    private static class PersonSerializer extends ProtobufSerializer<Person> {

        PersonSerializer() {
            super(Person.class, PERSON_TYPE_ID);
        }
    }

    private static class AnimalSerializerHook extends ProtobufSerializerHook<Animal> {

        AnimalSerializerHook() {
            super(Animal.class, ANIMAL_TYPE_ID);
        }
    }
}
