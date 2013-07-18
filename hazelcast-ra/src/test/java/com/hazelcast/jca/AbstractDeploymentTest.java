/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jca;

import java.util.UUID;

import javax.annotation.Resource;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.jboss.shrinkwrap.api.spec.ResourceAdapterArchive;


import static org.junit.Assert.assertNotNull;

/**
 * base class for jca tests
 *
 * @author asimarslan
 */
public class AbstractDeploymentTest {

    private final static String deploymentName="hazelcast-ra";


    @Deployment
    public static ResourceAdapterArchive deploymentHzRar(){
        ResourceAdapterArchive raa = ShrinkWrap.create(ResourceAdapterArchive.class, deploymentName + ".rar");

        JavaArchive ja = ShrinkWrap.create(JavaArchive.class, UUID.randomUUID().toString() + ".jar");
        ja.addPackage(JcaBase.class.getPackage());

        raa.addAsLibrary(ja);

        raa.addAsManifestResource("ra.xml","ra.xml");
        raa.addAsManifestResource("ironjacamar.xml","ironjacamar.xml");

        return raa;
    }

    @Resource(mappedName = "java:/HazelcastCF")
    protected HazelcastConnectionFactory connectionFactory;

    protected HazelcastConnection getConnection() throws Throwable{
        assertNotNull(connectionFactory);
        HazelcastConnection c = connectionFactory.getConnection();
        assertNotNull(c);
        return c;
    }

}
