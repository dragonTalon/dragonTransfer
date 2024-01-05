package com.dragon.transfer.common.exception;

import com.dragon.transfer.common.exception.code.ErrorCode;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @Title
 * @Author dragon
 * @Description
 * @Date 2023/11/20 10:23
 **/
public class DataOperationException extends RuntimeException {
    private ErrorCode errorCode;

    private DataOperationException(ErrorCode errorCode, String errorMessage, Throwable cause) {
        super(errorCode.toString() + " - " + getMessage(errorMessage) + " - " + getMessage(cause), cause);

        this.errorCode = errorCode;
    }

    public static DataOperationException asException(ErrorCode errorCode, Throwable cause) {
        if (cause instanceof DataOperationException) {
            return (DataOperationException) cause;
        }
        return new DataOperationException(errorCode, getMessage(cause), cause);
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
