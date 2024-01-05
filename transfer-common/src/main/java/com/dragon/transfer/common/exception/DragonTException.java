package com.dragon.transfer.common.exception;

import com.dragon.transfer.common.exception.code.ErrorCode;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @Title
 * @Author dragon
 * @Description
 * @Date 2023/11/28 16:33
 **/
public class DragonTException extends RuntimeException {
    private ErrorCode errorCode;

    public DragonTException(ErrorCode errorCode) {
        super(errorCode.toString());
        this.errorCode = errorCode;
    }

    public DragonTException(ErrorCode errorCode, String errorMessage) {
        super(errorCode.toString() + " - " + errorMessage);
        this.errorCode = errorCode;
    }

    public DragonTException(ErrorCode errorCode, String errorMessage, Throwable cause) {
        super(errorCode.toString() + " - " + getMessage(errorMessage) + " - " + getMessage(cause), cause);

        this.errorCode = errorCode;
    }

    public static DragonTException asException(ErrorCode errorCode, String message, Exception e) {

        return new DragonTException(errorCode, message, e);
    }

    public static DragonTException asException(ErrorCode errorCode, String message) {

        return new DragonTException(errorCode, message);
    }

    public static DragonTException asException(ErrorCode errorCode, Exception e) {

        return new DragonTException(errorCode, "", e);
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
