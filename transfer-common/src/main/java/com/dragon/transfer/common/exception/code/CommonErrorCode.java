package com.dragon.transfer.common.exception.code;

/**
 * @Title
 * @Author dragon
 * @Description
 * @Date 2023/11/28 16:34
 **/
public enum CommonErrorCode implements ErrorCode {
    /**
     * 异常整理
     */
    UNKNOWN_ERROR("0000", "位置异常."),
    CONVERT_NOT_SUPPORT("0001", "数据转换错误"),
    UNSUPPORTED_TYPE("0002", "不支持的数据库类型."),
    ILLEGAL_OPT("0003", "非法的操作"),
    CONVERT_OVER_FLOW("0004", "数据溢出"),
    ARGUMENT_ERROR("0005", "内部编程错误引起错误"),
    ;


    private final String code;

    private final String describe;

    CommonErrorCode(String code, String describe) {
        this.code = code;
        this.describe = describe;
    }


    @Override
    public String getCode() {
        return "COMMON_ERROR_" + this.code;
    }

    @Override
    public String getDescription() {
        return this.describe;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Describe:[%s]", this.code,
                this.describe);
    }
}
