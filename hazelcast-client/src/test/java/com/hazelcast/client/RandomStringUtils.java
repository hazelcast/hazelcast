package com.hazelcast.client;

public class RandomStringUtils {

	public static String random(int maxChars) {
		StringBuilder builder = new StringBuilder();
		for(int i=0;i<maxChars;i++){
			builder.append((int)Math.rint(Math.random()*10));
		}
		return builder.toString();
	}
}
