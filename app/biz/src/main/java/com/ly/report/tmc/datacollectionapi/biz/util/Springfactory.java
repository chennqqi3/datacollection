
package com.ly.report.tmc.datacollectionapi.biz.util;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;

import javax.annotation.Resource;

/**
 * Created by hyz46086 on 2017/5/10.
 */
public class Springfactory implements BeanFactoryAware{
    
    /** Spring容器 */
    private static BeanFactory beanFactory;  
    /** 
     * 回调方法 
     * @see BeanFactoryAware#setBeanFactory(BeanFactory)
     */
    @Override
    public void setBeanFactory(BeanFactory factory) throws BeansException {  
        // TODO Auto-generated method stub  
        beanFactory = factory;  
    }
    /**
     * 从Spring中获取指定名字的BEAN
     * @param beanName beanName
     * @param clazz 类型
     * @return Object
     */
    @SuppressWarnings("unchecked")
    public static Object getBean(String beanName, @SuppressWarnings("rawtypes") Class clazz){  
        
        return beanFactory.getBean(beanName, clazz);  
          
    }
}
      
    
