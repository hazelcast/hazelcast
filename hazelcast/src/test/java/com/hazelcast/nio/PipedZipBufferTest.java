package com.hazelcast.nio;

import java.util.Arrays;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.junit.Assert;
import org.junit.Test;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigXmlGenerator;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.nio.PipedZipBufferFactory.DeflatingPipedBuffer;
import com.hazelcast.nio.PipedZipBufferFactory.InflatingPipedBuffer;

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

}
