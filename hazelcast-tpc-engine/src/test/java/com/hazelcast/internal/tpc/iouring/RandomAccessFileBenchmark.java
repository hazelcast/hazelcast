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

package com.hazelcast.internal.tpc.iouring;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

public class RandomAccessFileBenchmark {

    private static final int sector = 4 * 1024;
    private static byte[] buf = new byte[sector];
    private static int duration = 30;

    public static final void main(String[] args) throws IOException {
        File file = new File(args[0]);
        long size = file.length();
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        Random rnd = new Random();
        long start = System.currentTimeMillis();
        long ios = 0;
        while (System.currentTimeMillis() - start < duration * 1000) {
            long pos = (long) (rnd.nextDouble() * (size >> 12));
            raf.seek(pos << 12);
            int count = raf.read(buf);
            ios++;
        }
        System.out.println("IOPS: " + ios / duration);
        long totalBytes = ios * sector;
        double totalSeconds = (System.currentTimeMillis() - start) / 1000.0;
        double speed = totalBytes / totalSeconds / 1024 / 1024;
        System.out.println(totalBytes + " bytes transferred in " + totalSeconds + " secs (" + speed + " MiB/sec)");
        raf.close();
    }
}
