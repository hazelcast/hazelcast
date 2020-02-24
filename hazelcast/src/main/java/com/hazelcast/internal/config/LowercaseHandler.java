package com.hazelcast.internal.config;

import java.util.HashMap;
import java.util.Map;

import com.hazelcast.internal.util.StringUtil;

public class LowercaseHandler {

	
	private Map<String, String> lowersMap;

	public LowercaseHandler() {
		this.lowersMap = new HashMap<>();
	}
	
	
	public void put(String value, boolean shouldLowercase)
	{
		if(value != null)
			lowersMap.put(shouldLowercase ? StringUtil.lowerCaseInternal(value) : value, value);
	}
	
	public String get(String value)
	{
		return lowersMap.get(value);
	}
	
	
}
