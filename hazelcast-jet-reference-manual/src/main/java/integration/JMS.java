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

import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import org.apache.activemq.ActiveMQConnectionFactory;

public class JMS {

    static void s1 () {
        //tag::s1[]
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.jmsQueue(() -> new ActiveMQConnectionFactory(
                "tcp://localhost:61616"), "queue"))
         .withoutTimestamps()
         .drainTo(Sinks.logger());
        //end::s1[]
    }

    static void s2 () {
        //tag::s2[]
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.jmsTopic(() -> new ActiveMQConnectionFactory(
                "tcp://localhost:61616"), "topic"))
         .withoutTimestamps()
         .drainTo(Sinks.logger());
        //end::s2[]
    }

    static void s3 () {
        //tag::s3[]
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.list("inputList"))
         .drainTo(Sinks.jmsQueue(() -> new ActiveMQConnectionFactory(
                 "tcp://localhost:61616"), "queue"));
        //end::s3[]
    }

    static void s4 () {
        //tag::s4[]
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.list("inputList"))
         .drainTo(Sinks.jmsTopic(() -> new ActiveMQConnectionFactory(
                 "tcp://localhost:61616"), "topic"));
        //end::s4[]
    }

}
