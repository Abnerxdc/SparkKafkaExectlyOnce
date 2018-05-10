package com.abner.dao;


import com.abner.bean.KafkaOffsetBean;
import com.abner.connection.MysqlConnection;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Admin on 2018/5/10.
 */
public class KafkaOffsetDao {
    public static final String tbl_kafka_offset = "tbl_kafka_offset";
    //表字段

    /**
     * 状态1 为 处理中
     * @param topic
     * @param partition
     * @param startOffset
     * @param untilOffset
     */
    public static boolean insetOffsetStatus(String topic , int partition , long startOffset , long untilOffset ,int status){
        String sql = String.format("insert into %s (`topic` ,`partition` , `startOffset` , `untilOffset` , `status`) " +
                "values ( '%s' , '%s'  , '%s' , '%s')",tbl_kafka_offset,topic,partition,startOffset,untilOffset,status);
        boolean returnFlag = false;
        try(Connection con = MysqlConnection.getConnection();
            Statement stmt = con.createStatement()){
            returnFlag = stmt.execute(sql);
        }catch (Exception e){
            e.printStackTrace();
        }
        return returnFlag;
    }
    public static boolean deleteOffsetStatus(String topic , int partition , long startOffset , long untilOffset ){
        String sql = String.format("delete from %s where `topic` = '%s' , `partition` = '%s'  ," +
                "`startOffset` = '%s' , `untilOffset` = '%s'",tbl_kafka_offset,topic,partition,startOffset,untilOffset);
        boolean returnFlag = false;
        try(Connection con = MysqlConnection.getConnection();
            Statement stmt = con.createStatement()){
            returnFlag = stmt.execute(sql);
        }catch (Exception e){
            e.printStackTrace();
        }
        return returnFlag;
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
