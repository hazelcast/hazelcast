/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.nio;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigXmlGenerator;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.nio.PipedZipBufferFactory.DeflatingPipedBuffer;
import com.hazelcast.nio.PipedZipBufferFactory.InflatingPipedBuffer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class PipedZipBufferTest {

    // config xml data
    private final byte[] data;
    private final int compressedDataLength;

    {
        final Config config = new Config();
        for (int i = 0; i < 1111; i++) {
            config.addMapConfig(new MapConfig("map:" + i));
            config.addQueueConfig(new QueueConfig().setName("queue:" + i));
        }
        data = new ConfigXmlGenerator(true).generate(config).getBytes();
        compressedDataLength = data.length / 9;
    }

    @Test
    public void deflatingBufferTest() throws Exception {
        DeflatingPipedBuffer defBuffer = PipedZipBufferFactory.createDeflatingBuffer(compressedDataLength);
        defBuffer.getOutputStream().write(data);
        int k = defBuffer.deflate();
        Assert.assertTrue("Compressed data should be smaller than actual data!", k < data.length);
        Inflater inf = new Inflater();
        inf.setInput(defBuffer.getOutputBuffer().array(), 0, k);
        byte[] data2 = new byte[data.length];
        int k2 = inf.inflate(data2);
        Assert.assertEquals("Inflated data length should be equal to actual data length!", data.length, k2);
        Assert.assertTrue("Inflated data should be equal to actual data!", Arrays.equals(data, data2));
    }

    @Test
    public void deflatingBufferTest2() throws Exception {
        DeflatingPipedBuffer defBuffer = PipedZipBufferFactory.createDeflatingBuffer(compressedDataLength);
        defBuffer.getInputBuffer().put(data);
        int k = defBuffer.deflate();
        Inflater inf = new Inflater();
        inf.setInput(defBuffer.getOutputBuffer().array(), 0, k);
        byte[] data2 = new byte[data.length];
        inf.inflate(data2);
        Assert.assertTrue(Arrays.equals(data, data2));
    }

    @Test
    public void inflatingBufferTest() throws Exception {
        Deflater def = new Deflater();
        def.setInput(data);
        def.finish();
        InflatingPipedBuffer infBuffer = PipedZipBufferFactory.createInflatingBuffer(compressedDataLength);
        int k = def.deflate(infBuffer.getInputBuffer().array());
        int k2 = infBuffer.inflate(k);
        Assert.assertEquals("Inflated data length should be equal to actual data length!", data.length, k2);
        byte[] data2 = new byte[data.length];
        System.arraycopy(infBuffer.getOutputBuffer().array(), 0, data2, 0, k2);
        Assert.assertTrue("Inflated data should be equal to actual data!", Arrays.equals(data, data2));
    }

    @Test
    public void pipedZipBufferTest() throws Exception {
        DeflatingPipedBuffer defBuffer = PipedZipBufferFactory.createDeflatingBuffer(compressedDataLength);
        defBuffer.getInputBuffer().put(data);
        int k = defBuffer.deflate();
        InflatingPipedBuffer infBuffer = PipedZipBufferFactory.createInflatingBuffer(compressedDataLength);
        infBuffer.getInputBuffer().put(defBuffer.getOutputBuffer());
        int k2 = infBuffer.inflate(k);
        Assert.assertEquals("Inflated data length should be equal to actual data length!", data.length, k2);
        byte[] data2 = new byte[data.length];
        System.arraycopy(infBuffer.getOutputBuffer().array(), 0, data2, 0, k2);
        Assert.assertTrue("Inflated data should be equal to actual data!", Arrays.equals(data, data2));
    }

    @Test
    public void invalidDataTest() throws Exception {
        DeflatingPipedBuffer defBuffer = PipedZipBufferFactory.createDeflatingBuffer(data.length);
        defBuffer.getInputBuffer().put(data);
        final int compressedDataLength = defBuffer.deflate();
        final byte[] compressedData = defBuffer.getOutputBuffer().array();
        InflatingPipedBuffer infBuffer = PipedZipBufferFactory.createInflatingBuffer(data.length);
        for (int i = 0; i < 13; i++) {
            infBuffer.reset();
            if (i % 2 == 0) {
                infBuffer.getInputBuffer().put(compressedData);
            } else {
                infBuffer.getInputBuffer().put(data);
            }
            try {
                int k2 = infBuffer.inflate(compressedDataLength);
                if (i % 2 == 0) {
                    Assert.assertEquals(data.length, k2);
                } else {
                    Assert.fail("Should fail here !!!");
                }
            } catch (Exception e) {
                if (i % 2 == 0) {
                    throw e;
                }
            }
        }
    }
}
