package com.ly.report.tmc.datacollectionapi.dal.hive.db;


import com.ly.report.tmc.datacollectionapi.dal.hive.db.to.OrderPassengerTick;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Created by hyz46086 on 2017/4/25.
 */
@Repository
public interface ChangeHiveDAO {

    List<OrderPassengerTick> getChangeOrderDepByChangeNo(String changeNo);
}
