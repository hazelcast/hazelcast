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

package com.hazelcast.jet.examples.spring;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.examples.spring.source.CustomSourceP;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.spring.context.SpringAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;

/**
 * Example of integrating Hazelcast Jet with Spring using an XML config.
 * We create the Spring context from an XML config using {@code application-context-with-schema.xml},
 * obtain a JetInstance bean from the context with the name 'instance' and submit a job.
 * <p>
 * Job uses a custom source implementation which has the {@link SpringAware}
 * annotation. This enables Spring to auto-wire beans to the created processors.
 */
public class XmlConfigurationWithSchemaSample {

    public static void main(String[] args) {
        ApplicationContext context = new GenericXmlApplicationContext("application-context-with-schema.xml");

        JetInstance jetInstance = (JetInstance) context.getBean("instance");
        JetInstance jetClient = (JetInstance) context.getBean("client");

        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(CustomSourceP.customSource())
                .writeTo(Sinks.logger());

        JobConfig jobConfig = new JobConfig()
                .addClass(XmlConfigurationWithSchemaSample.class)
                .addClass(CustomSourceP.class);
        jetClient.newJob(pipeline, jobConfig).join();

        jetClient.shutdown();
        jetInstance.shutdown();
    }
}
