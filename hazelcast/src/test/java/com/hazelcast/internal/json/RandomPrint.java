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

package com.hazelcast.internal.json;

import java.io.IOException;
import java.io.Writer;
import java.util.Random;

/**
 * Converts a JsonValue to a string. It adds whitespaces randomly. The
 * resulting string is functionally equal to the version without any
 * whitespace in it.
 */
public class RandomPrint extends WriterConfig {

    public static final RandomPrint RANDOM_PRINT = new RandomPrint(5);

    private final int atMostRandomWhitespace;

    public RandomPrint(int atMostRandomWhitespace) {
        this.atMostRandomWhitespace = atMostRandomWhitespace;
    }

    @Override
    protected JsonWriter createWriter(Writer writer) {
        return new RandomJsonPrinter(writer, atMostRandomWhitespace);
    }

    private static class RandomJsonPrinter extends JsonWriter {

        private final int atMostWeirdWhitespaces;

        private Random random = new Random();

        private RandomJsonPrinter(Writer writer, int atMostWeirdWhitespaces) {
            super(writer);
            this.atMostWeirdWhitespaces = atMostWeirdWhitespaces;
        }

        @Override
        protected void writeArrayOpen() throws IOException {
            writeRandomWhitespace();
            writer.write('[');
        }

        @Override
        protected void writeArrayClose() throws IOException {
            writeRandomWhitespace();
            writer.write(']');
        }

        @Override
        protected void writeArraySeparator() throws IOException {
            writeRandomWhitespace();
            writer.write(',');
        }

        @Override
        protected void writeObjectOpen() throws IOException {
            writeRandomWhitespace();
            writer.write('{');
        }

        @Override
        protected void writeObjectClose() throws IOException {
            writeRandomWhitespace();
            writer.write('}');
        }

        @Override
        protected void writeMemberSeparator() throws IOException {
            writeRandomWhitespace();
            writer.write(':');
        }

        @Override
        protected void writeObjectSeparator() throws IOException {
            writeRandomWhitespace();
            writer.write(',');
        }

        private void writeRandomWhitespace() throws IOException {
            int numberOfWhitespace = random.nextInt(atMostWeirdWhitespaces);
            for (int i = 0; i < numberOfWhitespace; i++) {
                switch (random.nextInt(3)) {
                    case 0:
                        writer.write(' ');
                        break;
                    case 1:
                        writer.write('\t');
                        break;
                    case 2:
                        writer.write('\n');
                        break;
                    default:
                        writer.write(' ');
                }
            }
        }
    }

}
