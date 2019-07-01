/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package expertzone;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorSupplier;

import javax.annotation.Nonnull;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

//tag::s1[]
class WriteFileP extends AbstractProcessor implements Closeable {

    private final String path;

    private transient BufferedWriter writer;

    WriteFileP(String path) {
        this.path = path;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        Path path = Paths.get(this.path, context.jetInstance().getName()
                + '-' + context.globalProcessorIndex());
        writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8);
    }

    @Override
    protected boolean tryProcess(int ordinal, Object item) throws Exception {
        writer.append(item.toString());
        writer.newLine();
        return true;
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }
}
//end::s1[]

//tag::s2[]
class WriteFilePSupplier implements ProcessorSupplier {

    private final String path;

    private transient List<WriteFileP> processors;

    WriteFilePSupplier(String path) {
        this.path = path;
    }

    @Override
    public void init(@Nonnull Context context) {
        File homeDir = new File(path);
        boolean success = homeDir.isDirectory() || homeDir.mkdirs();
        if (!success) {
            throw new JetException("Failed to create " + homeDir);
        }
    }

    @Override @Nonnull
    public List<WriteFileP> get(int count) {
        processors = Stream.generate(() -> new WriteFileP(path))
                           .limit(count)
                           .collect(Collectors.toList());
        return processors;
    }

    @Override
    public void close(Throwable error) {
        try {
            for (WriteFileP p : processors) {
                p.close();
            }
        } catch (IOException e) {
            throw new JetException(e);
        }
    }
}
//end::s2[]
