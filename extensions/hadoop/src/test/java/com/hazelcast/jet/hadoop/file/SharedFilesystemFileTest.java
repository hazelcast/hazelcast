/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.hadoop.file;

import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSources;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SharedFilesystemFileTest extends BaseFileFormatTest {

    @Test
    public void shouldReadFileOnlyOnceOnSharedFilesystem() {
        FileSourceBuilder<String> source = FileSources.files(currentDir + "/src/test/resources/directory/")
                                                      .format(FileFormat.lines())
                                                      .sharedFileSystem(true);

        assertItemsInSource(2, source, (collected) -> assertThat(collected).hasSize(4));

        source = FileSources.files(currentDir + "/src/test/resources/directory/")
                                                      .format(FileFormat.lines())
                                                      .sharedFileSystem(false);

        assertItemsInSource(2, source, (collected) -> assertThat(collected).hasSize(8));
    }
}
