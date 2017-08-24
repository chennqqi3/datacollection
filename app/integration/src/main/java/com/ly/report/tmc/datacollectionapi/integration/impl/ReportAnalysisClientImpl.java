package com.ly.report.tmc.datacollectionapi.integration.impl;

import java.lang.reflect.InvocationTargetException;

import javax.annotation.Resource;

import org.springframework.stereotype.Service;

import com.ly.flight.tmc.reportanalysisapi.facade.AirCodeAnalysisFacade;
import com.ly.flight.tmc.reportanalysisapi.facade.AirRouteFacade;
import com.ly.flight.tmc.reportanalysisapi.facade.DaysAheadFacade;
import com.ly.flight.tmc.reportanalysisapi.facade.DepartmentFacade;
import com.ly.flight.tmc.reportanalysisapi.facade.DepartureTimeFacade;
import com.ly.flight.tmc.reportanalysisapi.facade.DiscountFacade;
import com.ly.flight.tmc.reportanalysisapi.facade.FlightClassFacade;
import com.ly.flight.tmc.reportanalysisapi.facade.PassengerFacade;
import com.ly.flight.tmc.reportanalysisapi.facade.ReportAnalysisFacade;
import com.ly.flight.tmc.reportanalysisapi.facade.TravelPolicyFacade;
import com.ly.flight.tmc.reportanalysisapi.facade.dto.DepartureTimeDTO;
import com.ly.flight.tmc.reportanalysisapi.facade.dto.RequestDTO;
import com.ly.flight.tmc.reportanalysisapi.facade.dto.TravelPolicyDTO;
import com.ly.report.tmc.datacollectionapi.integration.ReportAnalysisClient;
import com.ly.sof.facade.base.ListResult;
import com.ly.sof.facade.base.SingleResult;
import org.springframework.stereotype.Service;

@Service("reportAnalysisClient")
public class ReportAnalysisClientImpl implements ReportAnalysisClient {
    @Resource
    private DiscountFacade        discountanalysisfacade;
    @Resource
    private AirCodeAnalysisFacade airCodeAnalysisFacade;
    @Resource
    private AirRouteFacade        airRouteFacade;
    @Resource
    private DaysAheadFacade       daysAheadFacade;
    @Resource
    private DepartmentFacade      departmentFacade;
    @Resource
    private DepartureTimeFacade   departureTimeFacade;
    @Resource
    private FlightClassFacade     flightClassFacade;
    @Resource
    private PassengerFacade       passengerFacade;
    @Resource
    private ReportAnalysisFacade  reportAnalysisFacade;
    @Resource
    private TravelPolicyFacade    travelPolicyFacade;

    /**
     * 折扣分析
     * @see com.ly.report.tmc.datacollectionapi.integration.ReportAnalysisClient#discountAnalysis(com.ly.flight.tmc.reportanalysisapi.facade.dto.RequestDTO)
     */
    @Override
    public ListResult<Long> discountAnalysis(RequestDTO dto) throws IllegalAccessException, InvocationTargetException {
        return discountanalysisfacade.discountAnalysis(dto);
    }

    /**
     * 航司分析
     * @see com.ly.report.tmc.datacollectionapi.integration.ReportAnalysisClient#airCodeAnalysis(com.ly.flight.tmc.reportanalysisapi.facade.dto.RequestDTO)
     */
    @Override
    public ListResult<Long> airCodeAnalysis(RequestDTO dto) {
        return airCodeAnalysisFacade.airCodeAnalysis(dto);
    }

    /**
     * 协议航空分析
     * @see com.ly.report.tmc.datacollectionapi.integration.ReportAnalysisClient#protocolAirCodeAnalysis(com.ly.flight.tmc.reportanalysisapi.facade.dto.RequestDTO)
     */
    @Override
    public ListResult<Long> protocolAirCodeAnalysis(RequestDTO dto) {
        return airCodeAnalysisFacade.protocolAirCodeAnalysis(dto);
    }

    /**
     * 航线分析
     * @see com.ly.report.tmc.datacollectionapi.integration.ReportAnalysisClient#airRouteCompute(com.ly.flight.tmc.reportanalysisapi.facade.dto.RequestDTO)
     */
    @Override
    public ListResult<Long> airRouteCompute(RequestDTO dto) {
        return airRouteFacade.airRouteCompute(dto);
    }

    /**
     * 提前天数分析
     * @see com.ly.report.tmc.datacollectionapi.integration.ReportAnalysisClient#computeDaysAhead(com.ly.flight.tmc.reportanalysisapi.facade.dto.RequestDTO)
     */
    @Override
    public ListResult<Long> computeDaysAhead(RequestDTO dto) {
        return daysAheadFacade.computeDaysAhead(dto);
    }

    /**
     * 部门分析
     * @see com.ly.report.tmc.datacollectionapi.integration.ReportAnalysisClient#computeDepartment(com.ly.flight.tmc.reportanalysisapi.facade.dto.RequestDTO)
     */
    @Override
    public ListResult<Long> computeDepartment(RequestDTO dto) {
        return departmentFacade.computeDepartment(dto);
    }

    /**
     * 起飞时间段分析
     * @see com.ly.report.tmc.datacollectionapi.integration.ReportAnalysisClient#departureTimeAnalysis(com.ly.flight.tmc.reportanalysisapi.facade.dto.RequestDTO)
     */
    @Override
    public SingleResult<DepartureTimeDTO> departureTimeAnalysis(RequestDTO dto) {
        return departureTimeFacade.departureTimeAnalysis(dto);
    }

    /**
     * 航等分析
     * @see com.ly.report.tmc.datacollectionapi.integration.ReportAnalysisClient#flightClassAnalysis(com.ly.flight.tmc.reportanalysisapi.facade.dto.RequestDTO)
     */
    @Override
    public ListResult<Long> flightClassAnalysis(RequestDTO dto) {
        return flightClassFacade.flightClassAnalysis(dto);
    }

    /**
     * 乘机人分析
     * @see com.ly.report.tmc.datacollectionapi.integration.ReportAnalysisClient#passengerAnalysis(com.ly.flight.tmc.reportanalysisapi.facade.dto.RequestDTO)
     */
    @Override
    public ListResult<Long> passengerAnalysis(RequestDTO dto) {
        return passengerFacade.passengerAnalysis(dto);
    }

    /**
     * 预订类型分析
     * @see com.ly.report.tmc.datacollectionapi.integration.ReportAnalysisClient#bookTypeAnalysis(com.ly.flight.tmc.reportanalysisapi.facade.dto.RequestDTO)
     */
    @Override
    public ListResult<Long> bookTypeAnalysis(RequestDTO dto) {
        return reportAnalysisFacade.bookTypeAnalysis(dto);
    }

    /**
     * 差旅政策分析
     * @see com.ly.report.tmc.datacollectionapi.integration.ReportAnalysisClient#travelPolicyAnalysis(com.ly.flight.tmc.reportanalysisapi.facade.dto.RequestDTO)
     */
    @Override
    public SingleResult<TravelPolicyDTO> travelPolicyAnalysis(RequestDTO dto) {
        return travelPolicyFacade.travelPolicyAnalysis(dto);
    }

}
