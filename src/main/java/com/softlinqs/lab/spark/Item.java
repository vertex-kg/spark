package com.softlinqs.lab.spark;

public class Item {

    private String runDate;
    private String key;
    private String value;

    public Item() {
    }

    public Item(String runDate, String key, String value) {
        this.runDate = runDate;
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getRunDate() {
        return runDate;
    }

    public void setRunDate(String runDate) {
        this.runDate = runDate;
    }
}
