package com.hazelcast.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

class Parser {
	private static final String SPLIT_EXPRESSION = " ";
	List<String> stack = new ArrayList<String>();
	Queue<String> output = new ArrayBlockingQueue<String>(100);
	Map<String,Integer> precedence = new HashMap<String,Integer>();
	public Parser() {
		precedence.put("(", 15);
		precedence.put(")", 15);
		precedence.put("not", 11);
		precedence.put("=", 10);
		precedence.put(">", 10);
		precedence.put("<", 10);
		precedence.put(">=", 10);
		precedence.put("<=", 10);
		precedence.put("==", 10);
		precedence.put("and", 9);
		precedence.put("or", 8);
	}
	
	public List<Object> toPrefix(String in){
//		String[] tokens = in.split(SPLIT_EXPRESSION);
		Object[] tokens = split(in).toArray();
		for (int i = 0; i < tokens.length; i++) {
			String token = (String)tokens[i];
			
			if(isOperand(token)){
				if(token.equals(")")){
					while(openParanthesesFound()){
						popStackToOutput();
					}
					stack.remove(stack.size()-1);
				}else{
					while(openParanthesesFound() &&!hasHigherPrecedence(token, stack.get(stack.size()-1))){
						popStackToOutput();
					}
					stack.add(token);
				}
			}
			else{
				output.add(token);
			}
		}

		while(stack.size()>0 ){
			popStackToOutput();
		}
		return Arrays.asList(output.toArray());
	}

	public List<String> split(String in) {
		StringBuilder result = new StringBuilder();
		List<String> charOperators = Arrays.asList("(",")","+","-","=","<",">","*","/");
		in.replaceAll("<=", " <= ");
		in.replaceAll(">=", " >= ");
		in.replaceAll(">=", " >= ");
		in.replaceAll("<>", " <> ");
		char[] chars = in.toCharArray();
		for (int i = 0; i < chars.length; i++) {
			char c = chars[i];
			if(charOperators.contains(String.valueOf(c))){
				if(i<chars.length - 2 && charOperators.contains(String.valueOf(chars[i+1])) && !("(".equals(String.valueOf(chars[i+1])) || ")".equals(String.valueOf(chars[i+1]))) ){
					result.append(" ").append(c).append(chars[i+1]).append(" ");
					i++;
				}
				else{
					result.append(" ").append(c).append(" ");					
				}
			}
			else{
				result.append(c);
			}
		}
		
		String[] tokens = result.toString().split(SPLIT_EXPRESSION);
		List<String> list = new ArrayList<String>();
		for (int i = 0; i < tokens.length; i++) {
			tokens[i] = tokens[i].trim();
			if(!tokens[i].equals("")){
				list.add(tokens[i]);
			}
		}
		return list;
	}

	private boolean openParanthesesFound() {
		return stack.size() > 0 && !stack.get(stack.size()-1).equals("(");
	}

	private void popStackToOutput() {
		output.add(stack.remove(stack.size()-1));
	}

	public boolean hasHigherPrecedence(String operator1, String operator2) {
		return precedence.get(operator1.toLowerCase()) > precedence.get(operator2.toLowerCase());
	}

	private boolean isOperand(String string) {
		return precedence.containsKey(string.toLowerCase());
	}
}
