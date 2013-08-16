package com.hazelcast.util;

import com.hazelcast.impl.Node;
import com.hazelcast.logging.ILogger;

import java.util.logging.Level;

public class MemoryMonitor extends Thread {
    private final  ILogger logger;
    private final Node node;

    public MemoryMonitor(Node node){
        super(node.threadGroup, node.getThreadNamePrefix("MemoryMonitor"));
        setDaemon(true);
        this.node = node;
        this.logger = node.getLogger(MemoryMonitor.class.getName());
    }

    public void run(){
        Runtime r = Runtime.getRuntime();

       while(node.isActive()){
             long free = r.freeMemory();
             long total = r.totalMemory();
             long used = total-free;
             long max = r.maxMemory();

             StringBuilder sb = new StringBuilder();
             sb.append("used=").append(bytesToString(used)).append(", ");
             sb.append("free=").append(bytesToString(free)).append(", ");
             sb.append("total=").append(bytesToString(total)).append(", ");
             sb.append("max=").append(bytesToString(max)).append(", ");
             sb.append("used/total=").append(percentage(used, total)).append(" ");
             sb.append("used/max=").append(percentage(used,max));
             logger.log(Level.INFO, sb.toString());

             try {
                 Thread.sleep(10000);
             } catch (InterruptedException e) {
                 return;
             }
         }
    }

    private static final String[] UNITS = new String[]{"", "K", "M", "G", "T", "P", "E"};

    public static String percentage(long x, long y){
        double p =100d*x/y;
        return String.format("%.2f",p)+"%";
    }

    public static String bytesToString(long bytes){
        for (int i = 6; i > 0; i--){
            double step = Math.pow(1024, i);
            if (bytes > step) return String.format("%3.1f%s", bytes / step, UNITS[i]);
        }
        return Long.toString(bytes);
    }
}
