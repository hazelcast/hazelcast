package com.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.management.metrics.CompressingProbeRenderer;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.test.HazelcastTestSupport;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

public class Main extends HazelcastTestSupport {

    public static void main(String[] args) throws Exception{
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();

        for(int k=0;k<5;k++){
            hz.getMap("foobar"+k).put("1","1");
        }

        Node node = getNode(hz);

        Thread.sleep(10000);

        MetricsRegistry registry = node.nodeEngine.getMetricsRegistry();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        DataOutputStream printWriter = new DataOutputStream(outputStream);
//        printWriter.writeLong(System.currentTimeMillis());
//        printWriter.writeUTF(node.address.toString());
//        printWriter.writeUTF(node.config.getGroupConfig().getName());
//        printWriter.flush();

        CompressingProbeRenderer probeRenderer = new CompressingProbeRenderer(10000);
        registry.render(probeRenderer);
        outputStream.write(probeRenderer.getRenderedBlob());
        //probeRenderer.flush();
        outputStream.flush();


        byte[] bytes = outputStream.toByteArray();
        System.out.println(bytes.length);
    }
}
