package com.dragon.transfer.core.utils;

import com.dragon.transfer.common.exception.code.ErrorCode;

/**
 * @Title
 * @Author dragon
 * @Description
 * @Date 2023/11/20 10:19
 **/
public enum DBUtilsErrorCode implements ErrorCode {

    WRITE_DATA_ERROR("0001", "插入目标数据库失败."),
    GET_COLUMN_INFO_FAILED("0002", "获取表字段相关信息失败."),
    ;

    private final String code;

    private final String description;

    DBUtilsErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return "DBUtils_ERROR_" + this.code;
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
