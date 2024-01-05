package com.dragon.transfer.common.exception.code;

import com.dragon.transfer.common.exception.code.ErrorCode;

/**
 * @Title
 * @Author dragon
 * @Description
 * @Date 2023/11/28 11:02
 **/
public enum TableErrorCode implements ErrorCode {
    /**
     * error
     */
    TABLE_FIELD_EMPTY("0001", "表中无结构"),
    TABLE_NOT_PRI("0002", "没有主键"),
    TABLE_EXEC_FAIL("0003", "数据同步执行失败"),
    TABLE_CREATE_FAIL("0004","数据库表创建失败"),

    ;


    private final String code;

    private final String description;

    TableErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return "TALEB_ERROR_" + this.code;
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
