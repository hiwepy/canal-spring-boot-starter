package com.alibaba.otter.canal.spring.boot.exception;

import org.apache.commons.lang.exception.NestableRuntimeException;

public class CanalConsumeException extends NestableRuntimeException {

    private static final long serialVersionUID = -7545341502620139031L;

    public CanalConsumeException(String errorCode){
        super(errorCode);
    }

    public CanalConsumeException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public CanalConsumeException(String errorCode, String errorDesc){
        super(errorCode + ":" + errorDesc);
    }

    public CanalConsumeException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode + ":" + errorDesc, cause);
    }

    public CanalConsumeException(Throwable cause){
        super(cause);
    }
}
