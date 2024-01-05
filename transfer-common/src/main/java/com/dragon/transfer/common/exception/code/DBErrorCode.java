package com.dragon.transfer.common.exception.code;

/**
 * @Title
 * @Author dragon
 * @Description
 * @Date 2023/11/28 11:02
 **/
public enum DBErrorCode implements ErrorCode {
    /**
     * error
     */
    DB_TABLE_EMPTY("0001", "数据库表中没有数据"),
    DB_CONN_FAIL("0002", "数据库连接失败"),
    DB_WRITE_FAIL("0003", "目标数据库写入失败"),
    DB_READ_FAIL("0004", "源数据库读取失败");



    private final String code;

    private final String description;

    DBErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return "DB_ERROR_" + this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
