package com.dragon.transfer.core.utils;

/**
 * @Title
 * @Author dragon
 * @Description
 * @Date 2023/11/28 14:43
 **/
public class BinLogSchema {
    private String file;

    private String position;

    public BinLogSchema(String file, String position) {
        this.file = file;
        this.position = position;
    }

    public String getFile() {
        return file;
    }

    public String getPosition() {
        return position;
    }

    @Override
    public String toString() {
        return "BinLogSchema{" +
                "file='" + file + '\'' +
                ", position='" + position + '\'' +
                '}';
    }
}
