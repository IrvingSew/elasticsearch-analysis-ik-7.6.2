package org.wltea.analyzer.util;


import com.alibaba.druid.pool.DruidDataSource;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;

import java.io.Serializable;
import java.sql.*;
import java.util.*;


@Data
public class JDBCUtils{

    @Value("${jdbc.driver}")
    private String drive;
    @Value("${jdbc.url}")
    private String url;
    @Value("${jdbc.username}")
    private String username;
    @Value("${jdbc.password}")
    private String password;

    private Connection connect;

    private DruidDataSource dataSource;
    private static final int maxActive = 30;
    private static final int initialSize = 20;
    private static final int maxWait = 6000;
    private static final int minIdle = 2;

    public JDBCUtils() {
//        new JDBCUtils("com.mysql.cj.jdbc.Driver","jdbc:mysql://192.168.1.129:3306/dataintell-des?useUnicode=true&allowMultiQueries=true&characterEncoding=utf8&characterSetResults=utf8&serverTimezone=Asia/Shanghai&zeroDateTimeBehavior=CONVERT_TO_NULL&useSSL=false","root","123456");
    }

    public JDBCUtils(String drive, String url, String username, String password) {
        this.drive = drive;
        this.url = url;
        this.username = username;
        this.password = password;
        DruidDataSource dataSource= new DruidDataSource();
        dataSource.setUrl(url);
        dataSource.setDriverClassName(drive);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setMaxActive(maxActive);
        dataSource.setInitialSize(initialSize);
        dataSource.setMaxWait(maxWait);
        dataSource.setMinIdle(minIdle);
        dataSource.setConnectionErrorRetryAttempts(5);
        dataSource.setBreakAfterAcquireFailure(true);
        this.dataSource = dataSource;
    }

    public Connection getConnection() throws SQLException {
        if(null == connect || connect.isClosed()){
            return this.dataSource.getConnection();
        }
        return connect;
    }

    /**
     * 查询多条记录
     *
     * @param sql
     * @param params
     * @return
     * @throws SQLException
     */
    public List<Map<String, Object>> findMoreResult(String sql, List<Object> params) {
        List<Map<String, Object>> list = new ArrayList<>();
        int index = 1;
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            connection = getConnection();
            pstmt = connection.prepareStatement(sql);
            if (params != null && !params.isEmpty()) {
                for (Object param : params) {
                    pstmt.setObject(index++, param);
                }
            }
            rs = pstmt.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            while (rs.next()) {
                Map<String, Object> map = new HashMap<>();
                for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    String col_name = rsmd.getColumnLabel(i + 1);
                    Object col_value = rs.getObject(col_name);
                    map.put(col_name, col_value);
                }
                list.add(map);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            release(rs, pstmt, connection);
        }
        return list;
    }

    /**
     * 获取list
     *
     * @return
     */
    public List<Map<String, Object>> list(String sql) {
        List<Map<String, Object>> list = new ArrayList<>();
        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            connection = getConnection();
            pstmt = connection.prepareStatement(sql);
            rs = pstmt.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            while (rs.next()) {
                Map<String, Object> map = new HashMap<>();
                for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    String col_name = rsmd.getColumnLabel(i + 1);
                    Object col_value = rs.getObject(col_name);
                    if(null != col_value && (!(col_value instanceof Serializable))){
                        col_value =  rs.getString(col_name);
                    }
                    map.put(col_name, col_value);
                }
                list.add(map);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            release(rs, pstmt, connection);
        }
        return list;
    }

    /**
     * 关闭并释放JDBC中资源对象
     *
     * @param closes
     */
    public final void release(AutoCloseable... closes) {
        if (closes != null && closes.length > 0) {
            for (AutoCloseable autoCloseable : closes) {
                if (autoCloseable != null) {
                    try {
                        autoCloseable.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
