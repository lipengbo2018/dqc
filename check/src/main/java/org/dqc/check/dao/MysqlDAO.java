package org.dqc.check.dao;

import org.dqc.check.util.ConfigLoader;
import org.dqc.check.util.DefaultConfigLoader;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MysqlDAO {

    private static final Logger logger = Logger.getLogger(MysqlDAO.class);

    private DefaultConfigLoader cfg;

    public MysqlDAO() {
        try {
            this.cfg = new ConfigLoader().getConfigInfo();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    public ArrayList getTaskConfig(String dt, Boolean isfirstexec) {

        ArrayList<TaskConfigDAO> taskconfiglist = new ArrayList<TaskConfigDAO>();
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String deletesql = "delete from dqc_check_log where dt='" + dt + "' and task_no like '1%'";
        String sql = null;
        if (isfirstexec) {
            sql = " SELECT * from dqc_check_config where is_delete=0 ";
        } else {
            sql = " SELECT * \n" +
                    "from dqc_check_config where is_delete=0\n" +
                    "and task_no not in (select task_no from dqc_check_log t \n" +
                    "where t.dt='" + dt + "' and t.exec_state=0) ";
        }

        try {

            if (isfirstexec) {
                //001 delete log
                conn = this.cfg.getUtils().getMySQLConfConnection();
                pstmt = conn.prepareStatement(deletesql);
                pstmt.execute();
            }

            //002 select data
            conn = this.cfg.getUtils().getMySQLConfConnection();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();

            while (rs.next()) {
                TaskConfigDAO taskconfig = new TaskConfigDAO();
                taskconfig.setROW_ID(rs.getInt("ROW_ID"));
                taskconfig.setTASK_NO(rs.getString("TASK_NO"));
                taskconfig.setTASK_DESC(rs.getString("TASK_DESC"));
                taskconfig.setTABLE_TYPE(rs.getString("TABLE_TYPE"));
                taskconfig.setTABLE_A(rs.getString("TABLE_A"));
                taskconfig.setTABLE_B(rs.getString("TABLE_B"));
                taskconfig.setFILTER_WHERE(rs.getString("FILTER_WHERE"));
                //taskconfig.setPARAM_DESC(rs.getString("PARAM_DESC")) ;
                taskconfig.setDIM_NAME(rs.getString("DIM_NAME"));
                taskconfig.setFACT_NAME(rs.getString("FACT_NAME"));
                taskconfig.setDIM_DESC(rs.getString("DIM_DESC"));
                taskconfig.setFACT_DESC(rs.getString("FACT_DESC"));
                taskconfiglist.add(taskconfig);
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            this.cfg.getUtils().release(rs, pstmt, conn);
        }

        return taskconfiglist;
    }

    public ArrayList getTaskConfigByTasknolist(String dt, String tasknolist, Boolean isfirstexec) {

        ArrayList<TaskConfigDAO> taskconfiglist = new ArrayList<TaskConfigDAO>();
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String taskno = null;
        String[] tasknoarr = tasknolist.split(",");
        for (int i = 0; i < tasknoarr.length; i++) {
            if (i == 0) {
                taskno = "('" + tasknoarr[i] + "'";
            } else {
                taskno = taskno + ",'" + tasknoarr[i] + "'";
            }
        }
        taskno = taskno + ")";

        String deletesql = "delete from dqc_check_log where ds='" + dt + "' and task_no like '1%' and task_no in " + taskno;
        String sql = null;
        if (isfirstexec) {
            sql = " SELECT * from dqc_check_config where is_delete=0 and task_no in " + taskno;
        } else {
            sql = " SELECT * \n" +
                    "from dqc_check_config where is_delete=0\n" +
                    "and task_no not in (select task_no from dqc_check_log t \n" +
                    "where t.dt='" + dt + "' and t.exec_state=0) and task_no in " + taskno;
        }
        try {
            if (isfirstexec) {
                //001 delete log
                conn = this.cfg.getUtils().getMySQLConfConnection();
                pstmt = conn.prepareStatement(deletesql);
                pstmt.execute();
            }
            //002 select data
            conn = this.cfg.getUtils().getMySQLConfConnection();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                TaskConfigDAO taskconfig = new TaskConfigDAO();
                taskconfig.setROW_ID(rs.getInt("ROW_ID"));
                taskconfig.setTASK_NO(rs.getString("TASK_NO"));
                taskconfig.setTASK_DESC(rs.getString("TASK_DESC"));
                taskconfig.setTABLE_TYPE(rs.getString("TABLE_TYPE"));
                taskconfig.setTABLE_A(rs.getString("TABLE_A"));
                taskconfig.setTABLE_B(rs.getString("TABLE_B"));
                taskconfig.setFILTER_WHERE(rs.getString("FILTER_WHERE"));
                //taskconfig.setPARAM_DESC(rs.getString("PARAM_DESC")) ;
                taskconfig.setDIM_NAME(rs.getString("DIM_NAME"));
                taskconfig.setFACT_NAME(rs.getString("FACT_NAME"));
                taskconfig.setDIM_DESC(rs.getString("DIM_DESC"));
                taskconfig.setFACT_DESC(rs.getString("FACT_DESC"));
                taskconfiglist.add(taskconfig);
            }
        } catch (Exception ex) {
            logger.info(ex.toString());
        } finally {
            this.cfg.getUtils().release(rs, pstmt, conn);
        }
        return taskconfiglist;
    }

    public ArrayList getIndicatorInfo(String dt, Boolean isfirstexec) {

        ArrayList<String> indicatorlist = new ArrayList<String>();
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        String deletesql = "delete from dqc_check_log where dt='" + dt + "' and task_no not like '1%'";
        String sql = null;
        if (isfirstexec) {
            sql = " SELECT indicator_name from dqc_check_indicator where is_delete=0 ";
        } else {
            sql = " SELECT indicator_name \n" +
                    "from dqc_check_indicator\n" +
                    "where indicator_name not in (select task_no from dqc_check_log t \n" +
                    "where t.dt='" + dt + "' and t.exec_state=0) ";
        }
        try {
            if (isfirstexec) {
                //001 delete log
                conn = this.cfg.getUtils().getMySQLConfConnection();
                pstmt = conn.prepareStatement(deletesql);
                pstmt.execute();
            }
            //002 select data
            conn = this.cfg.getUtils().getMySQLConfConnection();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String indicator = new String();
                indicator = rs.getString("indicator_name");
                indicatorlist.add(indicator);
            }
        } catch (Exception ex) {
            logger.error(ex.toString());
        } finally {
            this.cfg.getUtils().release(rs, pstmt, conn);
        }
        return indicatorlist;
    }

    public void insertCheckRst(String[] paramarr) {

        String[] logarr = new String[5];
        logarr[0] = paramarr[1];
        logarr[1] = paramarr[0];
        logarr[2] = paramarr[2];
        logarr[4] = null;
        Connection conn = null;
        Connection connselect = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String deletesql = "delete from dqc_check_result where task_no='" +
                paramarr[0] + "' and ds='" + paramarr[1] + "'";
        String selectsql = paramarr[2];
        String loadsql = "load data local infile '' " + "into table dqc_check_result fields terminated by ','";

        try {
            //001 ????????????
            connselect = this.cfg.getUtils().getMySQLDataConnection();
            pstmt = connselect.prepareStatement(selectsql);
            logger.info("[" + paramarr[0] + "][" + paramarr[1] + "] " + selectsql);
            rs = pstmt.executeQuery();
            //002 ???????????????????????????
            conn = this.cfg.getUtils().getMySQLConfConnection();
            conn.setAutoCommit(false);// ??????JDBC???????????????????????????
            pstmt = conn.prepareStatement(deletesql);
            logger.info("[" + paramarr[0] + "][" + paramarr[1] + "] " + deletesql);
            pstmt.execute();

            //003 insert???????????? ???LOAD???COMMIT
            pstmt = conn.prepareStatement(loadsql);
            StringBuilder sb = new StringBuilder();
            Integer i = 0;
            Integer commit_size = 2000;
            while (rs.next()) {
                sb.append(rs.getString("ds")
                        + "," + rs.getString("task_no")
                        + "," + rs.getString("dim_list")
                        + "," + rs.getString("fact_list")
                        + "," + rs.getString("fact_desc")
                        + "," + rs.getString("dim_name")
                        + "," + rs.getString("create_time")
                        + "\n");
                i++;
                if (i % commit_size == 0) {
                    InputStream is = new ByteArrayInputStream(sb.toString().getBytes());
                    ((com.mysql.jdbc.Statement) pstmt).setLocalInfileInputStream(is);
                    pstmt.execute();
                    sb.setLength(0);
                }
            }
            InputStream is = new ByteArrayInputStream(sb.toString().getBytes());
            ((com.mysql.jdbc.Statement) pstmt).setLocalInfileInputStream(is);
            logger.info("[" + paramarr[0] + "][" + paramarr[1] + "] " + loadsql);
            pstmt.execute();

            conn.commit();//??????JDBC??????
            conn.setAutoCommit(true);// ??????JDBC???????????????????????????
            logarr[3] = "0";
        } catch (Exception e) {
            logarr[3] = "-1";
            logarr[4] = e.toString();
            logger.error("[" + paramarr[0] + "][" + paramarr[1] + "] " + e.toString());
            try {
                conn.rollback();
                conn.setAutoCommit(true);
            } catch (Exception es) {
                logger.error("[" + paramarr[0] + "][" + paramarr[1] + "] " + es.toString());
            }
        } finally {
            this.cfg.getUtils().release(null, pstmt, conn);
            try {
                new MysqlDAO().insertCheckLog(logarr);
            } catch (Exception e) {
                logger.error(e.toString());
            }
        }
    }

    public void insertCheckLog(String[] logarr) {

        Connection conn = null;
        Connection connselect = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String insertsql = "insert into dqc_check_log(dt,task_no,exec_sql,exec_state,error_info) values (?,?,?,?,?)";

        try {
            conn = this.cfg.getUtils().getMySQLConfConnection();
            pstmt = conn.prepareStatement(insertsql);
            pstmt.setString(1, logarr[0]);
            pstmt.setString(2, logarr[1]);
            pstmt.setString(3, logarr[2]);
            pstmt.setString(4, logarr[3]);
            pstmt.setString(5, logarr[4]);
            pstmt.execute();
        } catch (SQLException e) {
            logger.error("[" + logarr[0] + "][" + logarr[1] + "] " + e.toString());
        } catch (ClassNotFoundException e) {
            logger.error("[" + logarr[0] + "][" + logarr[1] + "] " + e.toString());
        } finally {
            this.cfg.getUtils().release(null, pstmt, conn);
        }
    }

    public void insertCheckRstIndicator(String dt, String indicator) {
        String deletesql = " delete from dqc_check_result_app where ds='${dt}' and indicator_name='${indicator}';\n";
        String insertsql = " insert into dqc_check_result_app(ds,indicator_name,task_no,rows_rate,apps_rate,rows_all_rate,apps_all_rate,rows_all,indicator_rows_all,indicator_rows_abn,apps_all,indicator_apps_all,indicator_apps_abn,appid\n" +
                "   ,create_time)\n" +
                "select \n" +
                " t.dt\n" +
                " ,'${indicator}' indicator_name\n" +
                " ,t.task_no\n" +
                " ,round(100*(case when indicator_rows_all=0 then 0 else t.indicator_rows_abn/indicator_rows_all end),2) rows_rate\n" +
                " ,round(100*(case when indicator_apps_all=0 then 0 else t.indicator_apps_abn/indicator_apps_all end),2) apps_rate\n" +
                " ,round(100*(case when indicator_rows_all=0 then 0 else t.indicator_rows_abn/rows_all end),2) rows_all_rate\n" +
                " ,round(100*(case when indicator_apps_all=0 then 0 else t.indicator_apps_abn/apps_all end),2) apps_all_rate\n" +
                " ,t.rows_all\n" +
                " ,t.indicator_rows_all\n" +
                " ,t.indicator_rows_abn\n" +
                " ,t.apps_all\n" +
                " ,t.indicator_apps_all\n" +
                " ,t.indicator_apps_abn\n" +
                " ,t.appid\n" +
                " ,now() create_time\n" +
                "from\n" +
                "(\n" +
                "select ds,task_no\n" +
                ",sum(case when ifnull(t.indicator_offline,0)>=10  or ifnull(t.indicator_realtime,0)>=10\n" +
                "          then 1 else 0 end) indicator_rows_all\n" +
                ",sum(case when (ifnull(t.indicator_offline,0)>=10  or ifnull(t.indicator_realtime,0)>=10\n" +
                ")\n" +
                "             and (ifnull(t.indicator_offline,-1)=0\n" +
                "                   or abs( (t.indicator_offline-t.indicator_realtime)/t.indicator_offline)>=0.1\n" +
                "                 ) \n" +
                "          then 1 else 0 end) indicator_rows_abn\n" +
                ",count(distinct case when ifnull(t.indicator_offline,0)>=10  or ifnull(t.indicator_realtime,0)>=10\n" +
                "          then appid else null end) indicator_apps_all\n" +
                ",count(distinct case when (ifnull(t.indicator_offline,0)>=10  or ifnull(t.indicator_realtime,0)>=10\n" +
                ")\n" +
                "             and (ifnull(t.indicator_offline,-1)=0\n" +
                "                   or abs( (t.indicator_offline-t.indicator_realtime)/t.indicator_offline)>=0.1\n" +
                "                 ) \n" +
                "          then appid else null end) indicator_apps_abn \n" +
                ",count(1) rows_all\n" +
                ",count(distinct appid) apps_all\n" +
                ",appid\n" +
                "from\n" +
                "(\n" +
                "  select substring_index(case when t.fact_list like '%[${indicator}:%' \n" +
                "                 then substring_index(substring_index(t.fact_list,'[${indicator}:',-1),']',1)\n" +
                "                 else null end,'|',1) indicator_offline\n" +
                "        ,substring_index(case when t.fact_list like '%[${indicator}:%' \n" +
                "                 then substring_index(substring_index(t.fact_list,'[${indicator}:',-1),']',1)\n" +
                "                 else null end,'|',-1) indicator_realtime\n" +
                "    ,t.dt,t.task_no,t.dim_list,t.fact_list,t.fact_desc\n" +
                "    ,substring_index(t.dim_list,'|',1) appid\n" +
                "    from dqc_check_result t \n" +
                "    where t.dt='${dt}' \n" +
                "    and (t.fact_desc like '%|${indicator}|%' or t.fact_desc like '${indicator}|%' \n" +
                "          or t.fact_desc like '%|${indicator}' )/*????????????????????????task_no*/\n" +
                "    /*and t.task_no='10001' and t.fact_list like '%${indicator}:%' */\n" +
                ") t \n" +
                "group by ds,task_no,appid\n" +
                ") t";
        insertsql = insertsql.replace("${dt}", dt).replace("${indicator}", indicator);
        deletesql = deletesql.replace("${dt}", dt).replace("${indicator}", indicator);

        //ds,task_no,exec_sql,exec_state,error_info
        String[] logarr = new String[5];
        logarr[0] = dt;
        logarr[1] = indicator;
        logarr[2] = insertsql;
        logarr[4] = null;
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            //001 ?????????????????????
            conn = this.cfg.getUtils().getMySQLConfConnection();
            conn.setAutoCommit(false);// ??????JDBC???????????????????????????
            pstmt = conn.prepareStatement(deletesql);
            logger.info("[" + dt + "][" + indicator + "] " + deletesql);
            pstmt.execute();

            //003 insert????????????
            pstmt = conn.prepareStatement(insertsql);
            pstmt.execute();

            conn.commit();//??????JDBC??????
            conn.setAutoCommit(true);// ??????JDBC???????????????????????????
            logarr[3] = "0";
        } catch (Exception e) {
            logarr[3] = "-1";
            logarr[4] = e.toString();
            logger.error("[" + dt + "][" + indicator + "] " + e.toString());
            try {
                conn.rollback();
                conn.setAutoCommit(true);
            } catch (Exception es) {
                logger.error("[" + dt + "][" + indicator + "] " + e.toString());
            }
        } finally {
            this.cfg.getUtils().release(null, pstmt, conn);
            try {
                new MysqlDAO().insertCheckLog(logarr);
            } catch (Exception e) {
                logger.error(e.toString());
            }
        }
    }

    public void insertCheckRstIndicator2(String dt) {
        String deletesql = " delete from dqc_check_result_indicator where ds='${dt}'";
        String insertsql = "insert into dqc_check_result_indicator\n" +
                "select ds,indicator_name,task_no\n" +
                ",round(if(sum(indicator_rows_all)=0,0,100*sum(indicator_apps_abn)/sum(indicator_rows_all)),2) rows_rate\n" +
                ",round(if(sum(indicator_apps_all)=0,0,100*sum(indicator_apps_abn)/sum(indicator_apps_all)),2) apps_rate\n" +
                ",round(if(sum(rows_all          )=0,0,100*sum(indicator_rows_abn)/sum(rows_all          )),2) rows_all_rate\n" +
                ",round(if(sum(apps_all          )=0,0,100*sum(indicator_apps_abn)/sum(apps_all          )),2) apps_all_rate\n" +
                ",sum(rows_all          ) rows_all\n" +
                ",sum(indicator_rows_all) indicator_rows_all\n" +
                ",sum(indicator_rows_abn) indicator_rows_abn\n" +
                ",sum(apps_all          ) apps_all\n" +
                ",sum(indicator_apps_all) indicator_apps_all\n" +
                ",sum(indicator_apps_abn) indicator_apps_abn\n" +
                ",now() create_time\n" +
                "from dqc_check_result_app t where t.dt='${dt}'\n" +
                "group by t.dt,t.indicator_name,t.task_no";
        insertsql = insertsql.replace("${dt}", dt);
        deletesql = deletesql.replace("${dt}", dt);

        //ds,task_no,exec_sql,exec_state,error_info
        String[] logarr = new String[5];
        logarr[0] = dt;
        logarr[1] = "all";
        logarr[2] = insertsql;
        logarr[4] = null;
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            //001 ?????????????????????
            conn = this.cfg.getUtils().getMySQLConfConnection();
            conn.setAutoCommit(false);// ??????JDBC???????????????????????????
            pstmt = conn.prepareStatement(deletesql);
            logger.info("[" + dt + "]" + deletesql);
            pstmt.execute();

            //003 insert????????????
            pstmt = conn.prepareStatement(insertsql);
            pstmt.execute();

            conn.commit();//??????JDBC??????
            conn.setAutoCommit(true);// ??????JDBC???????????????????????????
            logarr[3] = "0";
        } catch (SQLException e) {
            logarr[3] = "-1";
            logarr[4] = e.toString();
            logger.error("[" + dt + "] " + e.toString());
            try {
                conn.rollback();
                conn.setAutoCommit(true);
            } catch (Exception es) {
                logger.error("[" + dt + "] " + e.toString());
            }
        } catch (ClassNotFoundException e) {
            logarr[3] = "-1";
            logarr[4] = e.toString();
            logger.error("[" + dt + "] " + e.toString());
            try {
                conn.rollback();
                conn.setAutoCommit(true);
            } catch (Exception es) {
                logger.error("[" + dt + "] " + e.toString());
            }
        } finally {
            this.cfg.getUtils().release(null, pstmt, conn);
            try {
                new MysqlDAO().insertCheckLog(logarr);
            } catch (Exception e) {
                logger.error(e.toString());
            }
        }
    }

    public void deleteCheckRst(String dt) {
        String resultsql1 = " delete from dqc_check_result where dt<='${dt}'";
        String resultsql4 = " delete from dqc_check_log where dt<='${dt}'";
        resultsql1 = resultsql1.replace("${dt}", dt);
        resultsql4 = resultsql4.replace("${dt}", dt);

        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = this.cfg.getUtils().getMySQLConfConnection();
            conn.setAutoCommit(false);// ??????JDBC???????????????????????????

            logger.info(resultsql1);
            pstmt = conn.prepareStatement(resultsql1);
            pstmt.execute();

            logger.info(resultsql4);
            pstmt = conn.prepareStatement(resultsql4);
            pstmt.execute();

            conn.commit();//??????JDBC??????
            conn.setAutoCommit(true);// ??????JDBC???????????????????????????
        } catch (Exception e) {
            logger.error("[" + dt + "] " + e.toString());
            try {
                conn.rollback();
                conn.setAutoCommit(true);
            } catch (Exception es) {
                logger.error("[" + dt + "] " + e.toString());
            }
        } finally {
            this.cfg.getUtils().release(null, pstmt, conn);
        }
    }

    public Map<String, StringBuilder> getWarningRst(String dt) {

        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "\n" +
                "select 'app' appid\n" +
                "    ,1 tasks\n" +
                "    ,2 indicators\n" +
                "    ,'111' task_list\n" +
                "    ,'222' indicator_list\n" +
                "";

        //Map checkrst=new HashMap<Integer,String>();
        Map<String, StringBuilder> sbmap = new HashMap<String, StringBuilder>();
        StringBuilder title = new StringBuilder();
        StringBuilder content = new StringBuilder();
        try {
            logger.info(sql);
            conn = this.cfg.getUtils().getMySQLConfConnection();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            Integer checkrstkey = 0;
            while (rs.next()) {
                checkrstkey++;
                if (checkrstkey == 1) {
                    content.append("<b>??????????????????: </b><br />");
                    content.append("<=========================================><br />");
                    content.append("<table border=\"1\" cellspacing=\"0\">");
                    content.append("<tr>");
                    content.append("<th>APPID</th>");
                    content.append("<th>???????????????</th>");
                    content.append("<th>???????????????</th>");
                    content.append("<th>??????????????????</th>");
                    content.append("<th>??????????????????</th>");
                    content.append("</tr>");
                }
                content.append("<tr>");
                content.append("<td>" + rs.getString("APPID") + "</td>");
                content.append("<td>" + rs.getString("INDICATORS") + "</td>");
                content.append("<td>" + rs.getString("TASKS") + "</td>");
                content.append("<td>" + rs.getString("INDICATOR_LIST") + "</td>");
                content.append("<td>" + rs.getString("TASK_LIST") + "</td>");
                content.append("</tr>");
            }
            if (checkrstkey > 0) {
                content.append("</table>");
                title.append("***" + dt + "????????????????????????APP??????'" + checkrstkey + "'??????????????????***");

                String sqlindicator = " select 'aaa' indicator_desc,'bbb' task_desc,1 maxrate, 2 rows_all_rate\n" +
                        ",3 rows_all,4 indicator_rows_abn,5 apps_all,6 indicator_apps_abn\n" +
                        "";

                pstmt = conn.prepareStatement(sqlindicator);
                rs = pstmt.executeQuery();
                Integer i = 1;
                while (rs.next()) {
                    if (i == 1) {
                        content.append("<b>??????????????????: </b><br />");
                        content.append("<=========================================><br />");
                        content.append("<table border=\"1\" cellspacing=\"0\">");
                        content.append("<tr>");
                        content.append("<th>??????</th>");
                        content.append("<th>??????</th>");
                        content.append("<th>??????</th>");
                        content.append("<th>??????</th>");
                        content.append("<th>??????</th>");
                        content.append("<th>?????????</th>");
                        content.append("<th>???APP???</th>");
                        content.append("<th>??????APP???</th>");
                        content.append("</tr>");
                    }
                    i++;
                    content.append("<tr>");
                    content.append("<td>" + rs.getString("INDICATOR_DESC") + "</td>");
                    content.append("<td>" + rs.getString("TASK_DESC") + "</td>");
                    content.append("<td>" + rs.getString("MAXRATE") + "</td>");
                    content.append("<td>" + rs.getString("ROWS_ALL_RATE") + "</td>");
                    content.append("<td>" + rs.getString("ROWS_ALL") + "</td>");
                    content.append("<td>" + rs.getString("INDICATOR_ROWS_ABN") + "</td>");
                    content.append("<td>" + rs.getString("APPS_ALL") + "</td>");
                    content.append("<td>" + rs.getString("INDICATOR_APPS_ABN") + "</td>");
                    content.append("</tr>");
                }
                if (i > 1) {
                    content.append("</table>");
                    title.append("?????????????????????'" + (i - 1) + "'");
                }
            }

        } catch (Exception ex) {
            logger.info(ex.toString());
        } finally {
            this.cfg.getUtils().release(rs, pstmt, conn);
        }
        sbmap.put("title", title);
        sbmap.put("content", content);
        return sbmap;
    }

    public Map<String, StringBuilder> getCheckRst(String dt) {

        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = " select t2.indicator_desc,t3.task_desc,t4.maxrate,t1.rows_all_rate\n" +
                ",t1.rows_all,t1.indicator_rows_abn,t1.apps_all,t1.indicator_apps_abn\n" +
                "from dqc_check_result_indicator t1\n" +
                "inner join dqc_check_indicator t2 on t1.indicator_name=t2.indicator_name\n" +
                "inner join dqc_check_config t3 on t1.task_no=t3.task_no\n" +
                "left join dqc_check_indicator_value t4 on t1.task_no=t4.task_no and t1.indicator_name=t4.indicator_name\n" +
                "where t1.ds='" + dt + "'";

        Map<String, StringBuilder> sbmap = new HashMap<String, StringBuilder>();
        StringBuilder title = new StringBuilder();
        StringBuilder content = new StringBuilder();
        try {

            conn = this.cfg.getUtils().getMySQLConfConnection();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            Integer checkrstkey = 1;
            while (rs.next()) {
                if (checkrstkey == 1) {
                    title.append(dt + "???????????????");
                    content.append("<b>??????????????????: </b><br />");
                    content.append("<=========================================><br />");
                    content.append("<table border=\"1\" cellspacing=\"0\">");
                    content.append("<tr>");
                    content.append("<th>??????</th>");
                    content.append("<th>??????</th>");
                    content.append("<th>??????</th>");
                    content.append("<th>??????</th>");
                    content.append("<th>??????</th>");
                    content.append("<th>?????????</th>");
                    content.append("<th>???APP???</th>");
                    content.append("<th>??????APP???</th>");
                    content.append("</tr>");
                }
                checkrstkey++;
                content.append("<tr>");
                content.append("<td>" + rs.getString("INDICATOR_DESC") + "</td>");
                content.append("<td>" + rs.getString("TASK_DESC") + "</td>");
                content.append("<td>" + rs.getString("MAXRATE") + "</td>");
                content.append("<td>" + rs.getString("ROWS_ALL_RATE") + "</td>");
                content.append("<td>" + rs.getString("ROWS_ALL") + "</td>");
                content.append("<td>" + rs.getString("INDICATOR_ROWS_ABN") + "</td>");
                content.append("<td>" + rs.getString("APPS_ALL") + "</td>");
                content.append("<td>" + rs.getString("INDICATOR_APPS_ABN") + "</td>");
                content.append("</tr>");
            }
            content.append("</table>");
        } catch (Exception ex) {
            logger.info(ex.toString());
        } finally {
            this.cfg.getUtils().release(rs, pstmt, conn);
        }
        sbmap.put("title", title);
        sbmap.put("content", content);
        return sbmap;
    }

    public Map<String, StringBuilder> getExecCheckTaskRst(String dt) {

        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = " select t1.task_no,t1.task_desc,t3.row_id\n" +
                ",if(t3.row_id is null,'no exec',ifnull(t3.error_info,'success')) error_info" +
                ",DATE_ADD(t3.create_time,INTERVAL 8 hour) create_time \n" +
                "from dqc_check_config t1\n" +
                "left join dqc_check_log t2 on t1.task_no=t2.task_no \n" +
                "   and t2.exec_state=0 and t2.dt='" + dt + "'\n" +
                "left join dqc_check_log t3 on t1.task_no=t3.task_no \n" +
                "   and t3.dt='" + dt + "'\n" +
                "where t1.is_delete=0 and t2.row_id is null\n" +
                "order by t1.task_no,t3.create_time";

        Map<String, StringBuilder> sbmap = new HashMap<String, StringBuilder>();
        StringBuilder title = new StringBuilder();
        StringBuilder content = new StringBuilder();
        try {
            //sql=new String(sql.getBytes("ISO-8859-1"),"UTF-8");//???????????????UTF-8
            conn = this.cfg.getUtils().getMySQLConfConnection();
            //conn = this.cfg.getUtils().getMySQLDataConnection();
            logger.info(sql);
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            Integer checkrstkey = 1;
            while (rs.next()) {
                if (checkrstkey == 1) {
                    title.append(dt + "?????????????????????????????????");
                }
                checkrstkey++;
                content.append("<tr>");
                content.append("<=========================================><br />");
                content.append("<b>??????: [" + rs.getString("TASK_NO") + "]" + rs.getString("TASK_DESC") + "</b><br />");
                content.append("<b>????????????: [" + rs.getString("CREATE_TIME") + "]</b><br />");
                content.append("<b>????????????: [" + rs.getString("ERROR_INFO") + "]</b><br />");
            }
        } catch (Exception ex) {
            logger.info(ex.toString());
        } finally {
            this.cfg.getUtils().release(rs, pstmt, conn);
        }
        sbmap.put("title", title);
        sbmap.put("content", content);
        return sbmap;
    }


}
