/**
 * LY.com Inc.
 * Copyright (c) 2004-2017 All Rights Reserved.
 */
package com.ly.report.tmc.datacollectionapi.biz.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author hyz46086
 * @version $Id: PropertiesUtil, v 0.1 2017/6/14 15:39 hyz46086 Exp $
 */
public class PropertiesUtil {

    private static ConcurrentHashMap<String, Properties> pops = new ConcurrentHashMap<String, Properties>();

    /**
     * @param config "config.properties"
     * @param charset "utf-8"
     * @return
     */
    public static Properties getProperties(String config, String charset) {

        Properties p = pops.get(config);
        if (p == null) {
            try {
                p = new Properties();
                p.load(new FileInputStream("classes/dubbo.properties"));

                //                p.load(new InputStreamReader(PropertiesUtil.class.getClassLoader().getResourceAsStream(config), charset));
                pops.put(config, p);
            } catch (IOException e) {

            }
        }
        return p;
    }

    public static Properties getProperties(String config) {
        return PropertiesUtil.getProperties(config, "utf-8");
    }

    public static Properties getProperties() {
        return PropertiesUtil.getProperties("config.properties");
    }
}
