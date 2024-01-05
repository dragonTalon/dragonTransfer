package com.dragon.transfer.common.enums;

/**
 * @author dragon
 */

public enum DBType {
    MYSQL("mysql", "com.mysql.jdbc.Driver"),
    ;

    private String desc;

    private String driverClass;

    DBType(String desc, String driverClass) {
        this.desc = desc;
        this.driverClass = driverClass;
    }

    public String getDesc() {
        return desc;
    }

    public String getDriverClass() {
        return driverClass;
    }
}
