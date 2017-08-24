package com.ly.report.tmc.datacollectionapi.biz.hive;

/**
 * Created by hyz46086 on 2017/4/20.
 */
public class HivePage {

    private int totalCount;
    private int totalPage;
    private long pageSize = 5000L;

    public int getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(int totalCount) {
        this.totalCount = totalCount;
    }

    public int getTotalPage() {
        return totalPage;
    }

    public void setTotalPage(int totalPage) {
        this.totalPage = totalPage;
    }

    public long getPageSize() {
        return pageSize;
    }

    public void setPageSize(long pageSize) {
        this.pageSize = pageSize;
    }
}
