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

import com.hazelcast.collection.IList;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.examples.spring.config.AppConfig;
import com.hazelcast.jet.examples.spring.dao.UserDao;
import com.hazelcast.jet.examples.spring.model.User;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.spring.JetSpringServiceFactories;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Example of using Spring beans as a service for {@link ServiceFactory}.
 * The pipeline reads the names from a list and maps each name to a {@link User}
 * using the Spring Bean obtained from the context.
 *
 * For more information and other usages see {@link JetSpringServiceFactories}
 */
public class BeanAsAServiceSample {

    private static final String LIST_NAME = "source-list";

    public static void main(String[] args) throws Exception {
        ApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class);
        JetInstance jet = context.getBean(JetInstance.class);

        fillList(jet);

        Pipeline pipeline = Pipeline.create();
        pipeline.<String>readFrom(Sources.list(LIST_NAME))
                .mapUsingService(JetSpringServiceFactories.bean(UserDao.class),
                        (userDao, item) -> userDao.findByName(item.toLowerCase()))
                .writeTo(Sinks.logger());

        jet.newJob(pipeline).join();

        jet.shutdown();
    }

    private static void fillList(JetInstance jet) {
        IList<String> list = jet.getList(LIST_NAME);
        if (list.isEmpty()) {
            list.add("JOE");
            list.add("ALICE");
            list.add("BOB");
            list.add("FOO");
        }
    }
}
