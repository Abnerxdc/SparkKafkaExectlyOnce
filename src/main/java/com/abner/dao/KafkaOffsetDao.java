package com.abner.dao;


import com.abner.bean.KafkaOffsetBean;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Admin on 2018/5/10.
 */
public class KafkaOffsetDao {
    public static final String tbl_kafka_offset = "tbl_kafka_offset";
    //表字段
//    public static final String TblTopic = "topic";
//    public static final String TblPartition = "partition";
//    public static final String TblStartOffset = "start_offset";
//    public static final String TblEndOffset = "end_offset";
//    public static final String TblStatus = "status";

    /**
     * 状态1 为 处理中
     * @param topic
     * @param partition
     * @param startOffset
     * @param endOffset
     */
    public static void insetOffsetStatus(String topic , int partition , long startOffset , long endOffset ,int status){
        String sql = String.format("insert into %s (`topic` ,`partition` , `startOffset` , `endOffset` , `status`) " +
                "values ( '%s' , '%s'  , '%s' , '%s')",tbl_kafka_offset,topic,partition,startOffset,endOffset,status);

    }
    public static void deleteOffsetStatus(String topic , int partition , long startOffset , long endOffset ){
        String sql = String.format("delete from %s where `topic` = '%s' , `partition` = '%s'  ," +
                "`startOffset` = '%s' , `endOffset` = '%s'",tbl_kafka_offset,topic,partition,startOffset,endOffset);

    }

    /**
     * 查询数据库中正在处理的offset  （程序终止前正在处理的信息）
     * @return
     */
    public static List<KafkaOffsetBean> selectOffsetStatus(){
        List<KafkaOffsetBean> list = new ArrayList<>();
        String sql = String.format("select * from %s where `status` = '1' ",tbl_kafka_offset);
        return list;
    }
}
