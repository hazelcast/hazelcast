/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.rest.service;

import com.hazelcast.internal.ascii.rest.InternalRestService;
import com.hazelcast.rest.HazelcastRestSpringApplication;
import com.hazelcast.rest.security.CustomSecurityContext;
import com.hazelcast.rest.util.NodeEngineImplHolder;
import com.hazelcast.security.impl.SecurityContextImpl;
import com.hazelcast.spi.impl.NodeEngineImpl;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;

public class RestServiceImpl implements InternalRestService {
    public RestServiceImpl(NodeEngineImpl nodeEngine) {
        SpringApplication application = new SpringApplication(HazelcastRestSpringApplication.class);
        application.setWebApplicationType(WebApplicationType.SERVLET);
        ConfigurableApplicationContext context = application.run();

        NodeEngineImplHolder nodeEngineImplHolder = context.getBean(NodeEngineImplHolder.class);
        nodeEngineImplHolder.setNodeEngine(nodeEngine);

        CustomSecurityContext securityContext = context.getBean(CustomSecurityContext.class);
        SecurityContextImpl securityContextImpl = new SecurityContextImpl(nodeEngine.getNode());
        securityContext.setSecurityContext(securityContextImpl);
    }
}
