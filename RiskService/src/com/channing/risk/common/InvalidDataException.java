package com.channing.risk.common;

/**
 * Created by IntelliJ IDEA.
 * User: John Channing
 * Date: 28-Jul-2010
 */
public class InvalidDataException extends Exception{
    public InvalidDataException(int row, String column, String errorMessage){
        super("Error found in data:" + errorMessage +
                " on row " + row + " with column: " + column);
    }

    public InvalidDataException(){
        super("Data is the wrong type");
    }

    public InvalidDataException(String message){
        super(message);
    }

}
