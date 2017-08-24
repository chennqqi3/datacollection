package com.ly.report.tmc.datacollectionapi.biz.exception;

import com.ly.sof.api.error.LYError;
import com.ly.sof.api.exception.LYException;

/**
 * Created by hyz46086 on 2017/4/17.
 */
public class BizException extends LYException {

    public BizException(LYError error) {
        super(error);
    }

    public BizException(LYError error, Throwable cause) {
        super(error, cause);
    }

    public BizException(Throwable cause) {
        super(cause);
    }
}
