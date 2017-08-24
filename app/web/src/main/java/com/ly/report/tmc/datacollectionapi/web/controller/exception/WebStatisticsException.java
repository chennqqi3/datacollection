package com.ly.report.tmc.datacollectionapi.web.controller.exception;

import com.ly.sof.api.error.LYError;
import com.ly.sof.api.exception.LYException;

/**
 * Created by hyz46086 on 2017/4/17.
 */
public class WebStatisticsException extends LYException {

    public WebStatisticsException(LYError error) {
        super(error);
    }

    public WebStatisticsException(LYError error, Throwable cause) {
        super(error, cause);
    }

    public WebStatisticsException(Throwable cause) {
        super(cause);
    }
}
