/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.server.HazelcastMemberStarter;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static com.hazelcast.test.OverridePropertyRule.set;
import static org.junit.Assert.assertEquals;

/**
 * Separate test because it cannot run in parallel with any other instance
 * tracking tests.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class InstanceTrackingServerModeTest extends HazelcastTestSupport {

    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule
    public final OverridePropertyRule configOverrideRule
            = set("hazelcast.config", "classpath:hazelcast-instance-tracking-test.xml");

    @After
    public void tearDown() {
        System.clearProperty("instance_tracking_filename");
        Hazelcast.shutdownAll();
    }

    @Test
    public void testServerMode() throws Exception {
        File tempFile = tempFolder.newFile();
        System.setProperty("instance_tracking_filename", tempFile.getAbsolutePath());

        HazelcastMemberStarter.main(new String[]{});

        String actualContents = new String(Files.readAllBytes(tempFile.toPath()), StandardCharsets.UTF_8);
        JsonObject json = Json.parse(actualContents).asObject();
        assertEquals("server", json.getString("mode", ""));
    }

}
