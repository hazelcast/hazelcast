package com.hazelcast.tpc.engine.iouring;

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
