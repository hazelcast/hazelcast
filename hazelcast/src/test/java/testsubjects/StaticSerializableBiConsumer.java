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

package testsubjects;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.function.BiConsumer;

public class StaticSerializableBiConsumer implements BiConsumer<String, Integer>, Serializable {

    private String outputFilePath;

    public StaticSerializableBiConsumer(String outputFilePath) {
        this.outputFilePath = outputFilePath;
    }

    @Override
    public void accept(String key, Integer value) {
        try {
            Path filePath = Paths.get(this.outputFilePath);
            String keyValue = key + "#" + value + "\n";
            Files.write(filePath, keyValue.getBytes(), StandardOpenOption.APPEND);
        } catch (IOException e) {
            //Test using this consumer should fail if desired
        }
    }

}
