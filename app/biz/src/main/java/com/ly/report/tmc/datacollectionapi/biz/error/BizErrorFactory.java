/**
 * LY.com Inc.
 * Copyright (c) 2004-2016 All Rights Reserved.
 */
package com.ly.report.tmc.datacollectionapi.biz.error;

import com.ly.sof.api.error.AbstractErrorFactory;
import com.ly.sof.api.error.LYError;

/**
 * 错误工厂类
 * @author jt39175
 * @version $Id: AirErrorFactory.java, v 0.1 2016年9月8日 上午9:40:49 jt39175 Exp $
 */
public class BizErrorFactory extends AbstractErrorFactory {

    /** 
     * @see AbstractErrorFactory#provideErrorBundleName()
     */
    @Override
    protected String provideErrorBundleName() {
        return "hotelwebapi-web";
    }

    /**
     * 获取RefundErrorFactory单例
     * 
     * @return
     */
    public static BizErrorFactory getInstance() {
        return AirErrorFactoryHolder.INSTANCE;
    }

    /**
     * 单例实现
     * RefundErrorFactory instance keeper
     * 
     * @author allen
     * @version $Id: RefundErrorFactoryHolder.java, v 0.1 2016年4月10日 下午2:53:49 allen Exp $
     */
    private static final class AirErrorFactoryHolder {
        /** instance */
        private static final BizErrorFactory INSTANCE = new BizErrorFactory();
    }

    /**
     * Token失效
     * 
     * @param
     * @return
     */
    public LYError sendAndProductError(String str) {
        return createError("LY0521101002", str);
    }



    /**
     * 分发任务失败
     *
     * @param
     * @return
     */
    public LYError assignTaskError(String str) {
        return createError("LY0521101003", str);
    }


    /**
     * 参数无效
     * @param param
     * @return
     */
    public LYError invalidParam(Object param) {
        return createError("LY0521027002", param);
    }

    /**
     * 未能获取到预订人信息
     * @param param
     * @return
     */
    public LYError noBookPersonInfoReturn(String token) {
        return createError("LY0521027003", token);
    }

    /**
     * 信用卡信息查询接口无法获得返回值
     * 
     * @param method
     * @return
     */
    public LYError getCardTypeListError(String method) {
        return createError("LY0521027004", method);
    }

    /**
     * 根据员工ID，查询员工详情失败
     * 
     * @param method
     * @return
     */
    public LYError queryEmployeeDetailByIdError(Long employeeId) {
        return createError("LY0521027005", employeeId);
    }
    
    /**
     * 根据token获取员工id失败
     * 
     * @param token
     * @return
     */
    public LYError getEmployeeIdByTokenError(String token){
        return createError("LY0521027006", token);
    }
}
