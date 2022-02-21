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

import com.hazelcast.config.Config;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.util.InstanceTrackingUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.function.Consumer;

import static com.hazelcast.internal.util.StringUtil.bytesToString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InstanceTrackingInfoTest extends HazelcastTestSupport {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testJsonFormat() throws IOException {
        assertTrackingFileContents(null, content -> {
            JsonObject json = Json.parse(content).asObject();
            // since we didn't start with HazelcastMemberStarter
            // the mode will be "embedded"
            assertEquals("embedded", json.getString("mode", ""));
            assertEquals("Hazelcast", json.getString("product", ""));
            assertEquals(0, json.getInt("licensed", -1));
        });
    }

    @Test
    public void testCustomFormat() throws IOException {
        String format = "mode: $HZ_INSTANCE_TRACKING{mode}\n"
                + "product: $HZ_INSTANCE_TRACKING{product}\n"
                + "licensed: $HZ_INSTANCE_TRACKING{licensed}\n"
                + "missing:$HZ_INSTANCE_TRACKING{missing}\n"
                + "broken: $HZ_INSTANCE_TRACKING{broken ";
        String expected = "mode: embedded\n"
                + "product: Hazelcast\n"
                + "licensed: 0\n"
                + "missing:$HZ_INSTANCE_TRACKING{missing}\n"
                + "broken: $HZ_INSTANCE_TRACKING{broken ";
        assertTrackingFileContents(format, content -> assertEquals(expected, content));
    }

    @Test
    public void testBrokenFormat() throws IOException {
        String format = "broken: $HZ_INSTANCE_TRACKING{broken \n mode: $HZ_INSTANCE_TRACKING{mode}";
        String expected = "broken: $HZ_INSTANCE_TRACKING{broken \n mode: $HZ_INSTANCE_TRACKING{mode}";
        assertTrackingFileContents(format, content -> assertEquals(expected, content));
    }

    @Test
    public void testCustomFileName() throws IOException {
        Config config = new Config();
        File tmpDir = tempFolder.newFolder();
        File trackingFile = new File(tmpDir,
                "hz-$HZ_INSTANCE_TRACKING{mode}-$HZ_INSTANCE_TRACKING{pid}-$HZ_INSTANCE_TRACKING{start_timestamp}.process");
        config.getInstanceTrackingConfig()
              .setEnabled(true)
              .setFileName(trackingFile.getAbsolutePath())
              .setFormatPattern("dummy");

        createHazelcastInstance(config);

        File[] files = tmpDir.listFiles((dir, name) -> name.startsWith("hz-embedded-"));
        assertNotNull(files);
        assertEquals(1, files.length);
        assertEquals("dummy", bytesToString(Files.readAllBytes(files[0].toPath())));
    }

    @Test
    public void whenInstanceTrackingEnabled_thenFileSetInSystemProperty() throws IOException {
        Config config = new Config();
        File tempFile = tempFolder.newFile();
        config.getInstanceTrackingConfig()
              .setEnabled(true)
              .setFileName(tempFile.getAbsolutePath());

        createHazelcastInstance(config);

        assertThat(System.getProperty(InstanceTrackingUtil.HAZELCAST_CONFIG_INSTANCE_TRACKING_FILE))
                .isEqualTo(tempFile.getAbsolutePath());
    }

    private void assertTrackingFileContents(String pattern, Consumer<String> contentAssertion) throws IOException {
        Config config = new Config();
        File tempFile = tempFolder.newFile();
        config.getInstanceTrackingConfig()
              .setEnabled(true)
              .setFileName(tempFile.getAbsolutePath())
              .setFormatPattern(pattern);

        createHazelcastInstance(config);

        String actualContents = new String(Files.readAllBytes(tempFile.toPath()), StandardCharsets.UTF_8);
        contentAssertion.accept(actualContents);
    }
}
