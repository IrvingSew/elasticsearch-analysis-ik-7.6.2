package org.wltea.analyzer.dic;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.springframework.beans.factory.annotation.Autowired;
import org.wltea.analyzer.help.ESPluginLoggerFactory;
import org.wltea.analyzer.util.JDBCUtils;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;


public class DatabaseMonitor implements Runnable {
    private static final Logger logger = ESPluginLoggerFactory.getLogger(DatabaseMonitor.class.getName());
    public static final String PATH_JDBC_PROPERTIES = "jdbc.properties";
    private static final String JDBC_URL = "jdbc.url";
    private static final String JDBC_USERNAME = "jdbc.username";
    private static final String JDBC_PASSWORD = "jdbc.password";
    private static final String JDBC_DRIVER = "jdbc.driver";
    private static final String SQL_UPDATE_MAIN_DIC = "jdbc.update.main.dic.sql";
    private static final String SQL_UPDATE_STOPWORD = "jdbc.update.stopword.sql";
    //更新间隔
    public final static String JDBC_UPDATE_INTERVAL = "jdbc.update.interval";

    private static final Timestamp DEFAULT_LAST_UPDATE = Timestamp.valueOf(LocalDateTime.of(LocalDate.of(2020, 1, 1), LocalTime.MIN));
    private static Timestamp lastUpdateTimeOfMainDic = null;
    private static Timestamp lastUpdateTimeOfStopword = null;

    private static DruidDataSource dataSource;
    private static final int maxActive = 10;
    private static final int initialSize = 1;
    private static final int maxWait = 6000;
    private static final int minIdle = 2;

    static {
        dataSource= new DruidDataSource();
        dataSource.setUrl(getUrl());
        dataSource.setDriverClassName(getDriver());
        dataSource.setUsername(getUsername());
        dataSource.setPassword(getPassword());
        dataSource.setMaxActive(maxActive);
        dataSource.setInitialSize(initialSize);
        dataSource.setMaxWait(maxWait);
        dataSource.setMinIdle(minIdle);
        dataSource.setConnectionErrorRetryAttempts(5);
        dataSource.setBreakAfterAcquireFailure(true);
    }

    public static String getUrl() {
        return Dictionary.getSingleton().getProperty(JDBC_URL);
    }
    public static String getUsername() {
        return Dictionary.getSingleton().getProperty(JDBC_USERNAME);
    }
    public static String getPassword() {
        return Dictionary.getSingleton().getProperty(JDBC_PASSWORD);
    }
    public static String getDriver() {
        return Dictionary.getSingleton().getProperty(JDBC_DRIVER);
    }
    public String getUpdateMainDicSql() {
        return Dictionary.getSingleton().getProperty(SQL_UPDATE_MAIN_DIC);
    }
    public String getUpdateStopWordSql() {
        return Dictionary.getSingleton().getProperty(SQL_UPDATE_STOPWORD);
    }

    /**
     * 加载MySQL驱动
     */
    public DatabaseMonitor() {
        logger.info("---------- DatabaseMonitor struct start ---------");
        SpecialPermission.check();
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            try {
                Class.forName(getDriver());
            } catch (ClassNotFoundException e) {
                logger.error("mysql jdbc driver not found", e);
            }
            return null;
        });
    }

    @Override
    public void run() {
        SpecialPermission.check();
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
//            Connection conn = null;
            try {
//                conn = DriverManager.getConnection(getUrl(), getUsername(), getPassword());
                //conn = dataSource.getConnection();
                // 更新主词典
                updateMainDic();
                // 更新停用词
                updateStopword();
            }catch (Exception e ){
                logger.error("failed to get connection", e);
            }finally {
//                if (conn != null) {
//                    try {
//                        conn.close();
//                    } catch (SQLException e) {
//                        logger.error("failed to close Connection", e);
//                    }
//                }
            }
            return null;
        });
    }


    /**
     * 主词典
     */
    public synchronized void updateMainDic() {
        Connection conn = null;
        logger.info("start update main dic");
        int numberOfAddWords = 0;
        int numberOfDisableWords = 0;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = dataSource.getConnection();
            String sql = getUpdateMainDicSql();
            Timestamp param = lastUpdateTimeOfMainDic == null ? DEFAULT_LAST_UPDATE : lastUpdateTimeOfMainDic;
            logger.info("param: " + param);
            ps = conn.prepareStatement(sql);
            ps.setTimestamp(1, param);
            rs = ps.executeQuery();
            while (rs.next()) {
                String word = rs.getString("word");
                word = word.trim();
                if (word.isEmpty()) {
                    continue;
                }
                lastUpdateTimeOfMainDic = rs.getTimestamp("update_time");
                if (rs.getBoolean("is_deleted")) {
//                    logger.info("[main dic] disable word: {}", word);
                    // 删除
                    Dictionary.disableWord(word);
                    numberOfDisableWords++;
                } else {
//                    logger.info("[main dic] add word: {}", word);
                    // 添加
                    Dictionary.addWord(word);
                    numberOfAddWords++;
                }
            }
            logger.info("end update main dic -> addWord: {}, disableWord: {}", numberOfAddWords, numberOfDisableWords);
        } catch (SQLException e) {
            logger.error("failed to update main_dic", e);
            // 关闭 ResultSet、PreparedStatement
        }finally {
            closeRsAndPs(rs, ps,conn);
        }
    }

    /**
     * 停用词
     */
    public synchronized void updateStopword() {
        Connection conn = null;
        logger.info("start update stopword");
        int numberOfAddWords = 0;
        int numberOfDisableWords = 0;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = dataSource.getConnection();
            String sql = getUpdateStopWordSql();
            Timestamp param = lastUpdateTimeOfStopword == null ? DEFAULT_LAST_UPDATE : lastUpdateTimeOfStopword;
            logger.info("param: " + param);
            ps = conn.prepareStatement(sql);
            ps.setTimestamp(1, param);
            rs = ps.executeQuery();
            while (rs.next()) {
                String word = rs.getString("word");
                word = word.trim();
                if (word.isEmpty()) {
                    continue;
                }
                lastUpdateTimeOfStopword = rs.getTimestamp("update_time");
                if (rs.getBoolean("is_deleted")) {
//                    logger.info("[stopword] disable word: {}", word);
                    // 删除
                    Dictionary.disableStopword(word);
                    numberOfDisableWords++;
                } else {
//                    logger.info("[stopword] add word: {}", word);
                    // 添加
                    Dictionary.addStopword(word);
                    numberOfAddWords++;
                }
            }
            logger.info("end update stopword -> addWord: {}, disableWord: {}", numberOfAddWords, numberOfDisableWords);
        } catch (SQLException e) {
            logger.error("failed to update main_dic", e);
        } finally {
            // 关闭 ResultSet、PreparedStatement
            closeRsAndPs(rs, ps,conn);
        }
    }

    public void closeRsAndPs(ResultSet rs, PreparedStatement ps,Connection conn) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                logger.error("failed to close ResultSet", e);
            }
        }
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                logger.error("failed to close PreparedStatement", e);
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error("failed to close Connection", e);
            }
        }
    }
}
