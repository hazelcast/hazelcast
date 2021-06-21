/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package childfirstclassloader;

import com.hazelcast.function.SupplierEx;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Reads contents of `childfirstclassloader/resource_test.txt` resource
 *
 * Depending on whether this class is loaded by system classloader or the processor classloader it will return
 * different content:
 * - system classloader returns content of `target/test-classes/childfirstclassloader/resource_test.txt` file
 * - processor classloader returns content of `childfirstclassloader/resource_test.txt` file inside resources.jar
 */
public class ResourceReader implements SupplierEx<String> {

    @Override
    public String getEx() throws Exception {
        InputStream is = ResourceReader.class.getClassLoader().getResourceAsStream("childfirstclassloader/resource_test.txt");
        return IOUtils.toString(is, UTF_8);
    }
}
