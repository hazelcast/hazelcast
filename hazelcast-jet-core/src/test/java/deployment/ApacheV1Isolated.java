/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package deployment;

import com.hazelcast.jet.AbstractProcessor;
import java.io.BufferedReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ApacheV1Isolated extends AbstractProcessor {

    @Override
    public boolean complete() {
        ClassLoader cl = getClass().getClassLoader();
        URL resource = cl.getResource("apachev1");
        assertNotNull(resource);
        BufferedReader reader = null;
        try {
            reader = Files.newBufferedReader(Paths.get(resource.toURI()));
            String firstLine = reader.readLine();
            String secondLine = reader.readLine();
            assertTrue(secondLine.contains("Version 1.1"));
            assertNull(cl.getResourceAsStream("apachev2"));

        } catch (IOException | URISyntaxException e) {
            fail();
        }
        return true;

    }
}
