package com.abner.connection;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Created by Admin on 2018/5/10.
 * @author xudacheng
 */
public class MysqlConnection {
    public static final String url = "jdbc:mysql://118.25.102.146:3306/bigdata?useUnicode=true&characterEncoding=utf-8&useSSL=false";
    public static final String username = "root";
    public static final String password = "xxx";
    public static Connection getConnection(){
        Connection connection ;
        try{
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(url,username,password);
        }catch (Exception e){
            connection = null;
            e.printStackTrace();
        }
        return connection;
    }
}
