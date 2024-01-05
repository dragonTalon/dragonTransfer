package com.dragon.transfer.common.element;

import com.dragon.transfer.common.exception.DragonTException;
import com.dragon.transfer.common.exception.code.CommonErrorCode;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/**
 * @Title
 * @Author dragon
 * @Description
 * @Date 2023/11/29 16:56
 **/
public class StructColum extends Column {

    public StructColum(final String struct) {
        super(struct, Type.STRUCT, struct.length());
    }

    @Override
    public Long asLong() {
        throw DragonTException.asException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
                        "struct[\"%s\"]不能转为Long .", this.asString()));
    }

    @Override
    public Double asDouble() {
        throw DragonTException.asException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
                        "struct[\"%s\"]不能转为Double .", this.asString()));
    }

    @Override
    public String asString() {
        return (String) getRawData();
    }

    @Override
    public Date asDate() {
        throw DragonTException.asException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
                        "struct[\"%s\"]不能转为DATE .", this.asString()));
    }

    @Override
    public Date asDate(String dateFormat) {
        throw DragonTException.asException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
                        "struct[\"%s\"]不能转为DATE .", this.asString()));
    }

    @Override
    public byte[] asBytes() {
        throw DragonTException.asException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
                        "struct[\"%s\"]不能转为Byte .", this.asString()));
    }

    @Override
    public Boolean asBoolean() {
        throw DragonTException.asException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
                        "struct[\"%s\"]不能转为BOOLEAN .", this.asString()));
    }

    @Override
    public BigDecimal asBigDecimal() {
        throw DragonTException.asException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
                        "struct[\"%s\"]不能转为BigDecimal .", this.asString()));
    }

    @Override
    public BigInteger asBigInteger() {
        throw DragonTException.asException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
                        "struct[\"%s\"]不能转为BigInteger .", this.asString()));
    }
}
