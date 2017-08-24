/**
 * LY.com Inc.
 * Copyright (c) 2004-2016 All Rights Reserved.
 */
package com.ly.report.tmc.datacollectionapi.web.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * 
 * @author allen
 * @version $Id: SampleController.java, v 0.1 2016年7月11日 下午10:12:13 allen Exp $
 */
@Controller
public class IndexController {

    @RequestMapping("/index.htm")
    public String sample() {
        return "index";
    }
}
