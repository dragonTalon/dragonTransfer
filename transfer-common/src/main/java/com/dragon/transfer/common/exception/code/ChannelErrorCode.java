package com.dragon.transfer.common.exception.code;

/**
 * @Title
 * @Author dragon
 * @Description
 * @Date 2023/11/30 15:02
 **/
public enum ChannelErrorCode implements ErrorCode {
    /**
     * 通道错误
     */
    CHANNEL_ERROR_STORE("0001", "通道数据存储是失败"),
    CHANNEL_GET_FAIL("0002", "获取通道错误"),
    ;

    private final String code;

    private final String describe;

    ChannelErrorCode(String code, String describe) {
        this.code = code;
        this.describe = describe;
    }


    @Override
    public String getCode() {
        return "CHANNEL_ERROR_" + this.code;
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
