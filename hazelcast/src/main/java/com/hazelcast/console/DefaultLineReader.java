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

package com.hazelcast.console;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A {@link LineReader} implementation.
 */
class DefaultLineReader implements LineReader {

    private final BufferedReader in;

    DefaultLineReader() {
        in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
    }

    @Override
    public String readLine() throws Exception {
        return interruptableReadLine(in);
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    private String interruptableReadLine(BufferedReader reader)
            throws InterruptedException, IOException {
        @SuppressWarnings("checkstyle:magicnumber")
        final long parkDurationInMs = 100;
        Pattern pattern = Pattern.compile("\\R");
        boolean interrupted = false;
        int chr;
        StringBuilder result = new StringBuilder();
        Matcher matcher = pattern.matcher(result.toString());
        while (!interrupted && !matcher.find()) {
            if (reader.ready()) {
                chr = reader.read();
                result.append((char) chr);
                matcher = pattern.matcher(result.toString());
            } else {
                // when input buffer is not ready, wait
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(parkDurationInMs));
            }
            interrupted = Thread.interrupted();
        }

        if (interrupted) {
            throw new InterruptedException();
        }

        return result.toString();
    }
}
