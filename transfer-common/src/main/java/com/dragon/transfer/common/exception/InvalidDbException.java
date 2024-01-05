package com.dragon.transfer.common.exception;

import com.dragon.transfer.common.exception.code.ErrorCode;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @Title
 * @Author dragon
 * @Description
 * @Date 2023/11/28 10:59
 **/
public class InvalidDbException extends RuntimeException {
    private ErrorCode errorCode;

    public InvalidDbException(ErrorCode errorCode) {
        super(errorCode.toString());
        this.errorCode = errorCode;
    }

    public InvalidDbException(ErrorCode errorCode, String errorMessage, Throwable cause) {
        super(errorCode.toString() + " - " + getMessage(errorMessage) + " - " + getMessage(cause), cause);

        this.errorCode = errorCode;
    }

    public static InvalidDbException asException(ErrorCode errorCode, Throwable cause) {
        if (cause instanceof InvalidDbException) {
            return (InvalidDbException) cause;
        }
        return new InvalidDbException(errorCode, getMessage(cause), cause);
    }

    public ErrorCode getErrorCode() {
        return this.errorCode;
    }

    private static String getMessage(Object obj) {
        if (obj == null) {
            return "";
        }

        if (obj instanceof Throwable) {
            StringWriter str = new StringWriter();
            PrintWriter pw = new PrintWriter(str);
            ((Throwable) obj).printStackTrace(pw);
            return str.toString();
        } else {
            return obj.toString();
        }
    }
}
