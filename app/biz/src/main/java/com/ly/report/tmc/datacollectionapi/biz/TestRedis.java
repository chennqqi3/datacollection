package com.ly.report.tmc.datacollectionapi.biz;

import com.ly.report.tmc.datacollectionapi.biz.redis.RedisUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by hyz46086 on 2017/5/11.
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:META-INF/spring/biz-beans.xml" })
public class TestRedis {

    @Test
    public void testRedis(){

        System.out.println( "111111111111111111");

        System.out.println( RedisUtils.getValue("changeNumFlag"));


    }
}
