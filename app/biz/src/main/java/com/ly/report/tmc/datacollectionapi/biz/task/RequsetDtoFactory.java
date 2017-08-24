/**
 * LY.com Inc.
 * Copyright (c) 2004-2017 All Rights Reserved.
 */
package com.ly.report.tmc.datacollectionapi.biz.task;

import com.ly.flight.tmc.reportanalysisapi.facade.dto.RequestDTO;
import com.ly.report.tmc.datacollectionapi.biz.zk.impl.PreliminaryNodeChildrenCallback;

/**
 * @author hyz46086
 * @version $Id: RequsetDtoFactory, v 0.1 2017/6/25 16:14 hyz46086 Exp $
 */
public class RequsetDtoFactory
{
    public static RequestDTO requestDTO=null;

    public static RequestDTO getRequestDTO(String startTime,String endTime){
        if (requestDTO == null) {
            synchronized (RequsetDtoFactory.class) {
                if (requestDTO == null) {
                    requestDTO = new RequestDTO( );
                    requestDTO.setStartDate(startTime);
                    requestDTO.setEndDate(endTime);
                    requestDTO.setId(1L);
                    requestDTO.setParentid(2L);
                    requestDTO.setState(0);
                }
            }
            return requestDTO;
        }
        if(startTime!=null && endTime!=null){
            requestDTO.setStartDate(startTime);
            requestDTO.setEndDate(endTime);

        }
        return requestDTO;
    }
}
