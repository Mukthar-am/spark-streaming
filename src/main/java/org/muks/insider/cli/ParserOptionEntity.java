package org.muks.insider.cli;

/**
 * Created by 15692 on 16/08/16.
 * Option entity class to store option records
 * */


public class ParserOptionEntity {
    String name, description;
    boolean hasArg;
    public ParserOptionEntity(String name, String desc, boolean hasArg) {
        this.name = name; this.description = desc; this.hasArg = hasArg;
    }
}