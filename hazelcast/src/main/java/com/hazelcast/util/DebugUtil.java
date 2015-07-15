package com.hazelcast.util;

import com.hazelcast.nio.IOUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Contains some debugging functionality; useful if you are running large testsuites and can't rely on a debugger.
 */
@SuppressFBWarnings({"DM_DEFAULT_ENCODING"})
public final class DebugUtil {

    private DebugUtil() {
    }

    /**
     * Prints the stacktrace of the calling thread to System.out.
     *
     * @param msg debug message
     */
    public static void printStackTrace(String msg) {
        try {
            throw new Exception(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Appends the stacktrace of the current thread to a file.
     * <p/>
     * If something fails while writing to the file, the exception is printed and then ignored.
     *
     * @param file the file to which the stacktrace of the current thread is appended.
     * @throws java.lang.NullPointerException if file is null.
     */
    public static void appendStackTrace(File file) {
        if (file == null) {
            throw new NullPointerException();
        }

        Thread thread = Thread.currentThread();

        StringBuffer sb = new StringBuffer();
        sb.append(thread.getClass());
        sb.append(' ');
        sb.append(thread.getName());
        sb.append('\n');
        StackTraceElement[] elements = thread.getStackTrace();
        for (StackTraceElement element : elements) {
            sb.append('\t').append(element.toString()).append('\n');
        }

        PrintWriter out = null;
        try {
            out = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));
            out.print(sb.toString());
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtil.closeResource(out);
        }
    }

    /**
     * Appends text to a file.
     * <p/>
     * If something fails while writing to the file, the exception is printed and then ignored.
     *
     * @param file the file to which text is appended.
     * @param s the text to append to the file.
     * @throws java.lang.NullPointerException if file is null
     */
    public static void appendWithNewLine(File file, String s) {
        if (file == null) {
            throw new NullPointerException();
        }

        PrintWriter out = null;
        try {
            out = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));
            if (s == null) {
                out.println("null");
            } else {
                out.println(s);
            }
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtil.closeResource(out);
        }
    }
}
