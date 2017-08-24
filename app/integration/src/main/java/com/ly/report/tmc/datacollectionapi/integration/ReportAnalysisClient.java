package com.ly.report.tmc.datacollectionapi.integration;

import java.lang.reflect.InvocationTargetException;

import com.ly.flight.tmc.reportanalysisapi.facade.dto.DepartureTimeDTO;
import com.ly.flight.tmc.reportanalysisapi.facade.dto.RequestDTO;
import com.ly.flight.tmc.reportanalysisapi.facade.dto.TravelPolicyDTO;
import com.ly.sof.facade.base.ListResult;
import com.ly.sof.facade.base.SingleResult;

/**
 * 接报表分析facade
 * 
 * @author hy45524
 * @version $Id: ReportAnalysisClient.java, v 0.1 2017年5月12日 下午1:57:07 hy45524 Exp $
 */
public interface ReportAnalysisClient {

    /*
     * 折扣分析
     */
    ListResult<Long> discountAnalysis(RequestDTO dto) throws IllegalAccessException, InvocationTargetException;

    /*
     * 航司分析
     */
    ListResult<Long> airCodeAnalysis(RequestDTO dto);

    /*
     * 协议航空分析
     */
    ListResult<Long> protocolAirCodeAnalysis(RequestDTO dto);

    /*
     * 航线分析
     */
    ListResult<Long> airRouteCompute(RequestDTO dto);

    /*
     * 提前天数分析
     */
    ListResult<Long> computeDaysAhead(RequestDTO dto);

    /*
     * 部门分析
     */
    ListResult<Long> computeDepartment(RequestDTO dto);

    /*
     * 起飞时间段分析
     */
    SingleResult<DepartureTimeDTO> departureTimeAnalysis(RequestDTO dto);

    /*
     * 航等分析
     */
    ListResult<Long> flightClassAnalysis(RequestDTO dto);

    /*
     * 乘机人分析
     */
    ListResult<Long> passengerAnalysis(RequestDTO dto);

    /*
     * 预订类型分析
     */
    ListResult<Long> bookTypeAnalysis(RequestDTO dto);

    /*
     * 差旅政策分析
     */
    SingleResult<TravelPolicyDTO> travelPolicyAnalysis(RequestDTO dto);

}
