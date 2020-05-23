package com.github.liuche51.easyTask.cluster.follow;

import com.github.liuche51.easyTask.dao.ScheduleBakDao;
import com.github.liuche51.easyTask.dto.ScheduleBak;
import com.github.liuche51.easyTask.dto.proto.ScheduleDto;
/**
 * Follow服务入口
 */
public class FollowService {
    /**
     * 接受leader同步任务入备库
     * @param schedule
     */
    public static void saveScheduleBak(ScheduleDto.Schedule schedule){
        ScheduleBak bak=ScheduleBak.valueOf(schedule);
        ScheduleBakDao.save(bak);
    }
}
