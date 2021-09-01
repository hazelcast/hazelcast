package com.hazelcast.jet;

import org.HdrHistogram.Histogram;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;

public class LogTimer {
    static final HashMap<String, Long> Timers = new HashMap<String, Long>();
    static final HashMap<String, Histogram> histograms = new HashMap<String, Histogram>();

    public static boolean active = false;
    public static boolean consoleOutput = false;

    public static void calculateSelfTime(String name) {
        active = true;
        start(name);
        stop(name);
        active = false;
    }

    public static void start(String timerName) {
        if (!active)
            return;
        if (!histograms.containsKey(timerName)) {
            Histogram histogram = new Histogram(3600000000L, 3);
            histograms.put(timerName, histogram);
        }
        long nanoTime = System.nanoTime();

        if (consoleOutput)
            System.out.println("ttt == " + timerName + " Started! time == " + nanoTime);

        Timers.put(timerName, nanoTime);
    }

    public static void stop(String timerName) {
        if (!active)
            return;

        long nanoTime = System.nanoTime();
        long prevTime = Timers.get(timerName);
        long resultNano = nanoTime - prevTime;

        histograms.get(timerName).recordValue(resultNano / 1000);

        if (consoleOutput)
            System.out.println("ttt == " + timerName + " took " + resultNano + " nanosecs\n\t\t"
                    + (resultNano / 1000) + " microSecs\n\t\t"
                    + (resultNano / 1000000) + " milliSecs\n\t\t"
                    + "Time == " + nanoTime);
    }

    public static void ExportHistograms() {
        for (String key : histograms.keySet()) {
            String outFileName = "percentiles/" + key + ".txt";
            File outFile = new File(outFileName);
            if (outFile.exists() && outFile.isFile())
            {
                outFile.delete();
            }
            try {
                outFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
            Histogram histogram = histograms.get(key);
            System.out.println("FFF Created Percentile File ==> "+outFile.getAbsolutePath());
            System.out.println("Mean Time for " + key + " ==> "+histogram.getMean());
            try (PrintStream ps = new PrintStream(outFile)) {
                histogram.outputPercentileDistribution(ps, 1.0);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
