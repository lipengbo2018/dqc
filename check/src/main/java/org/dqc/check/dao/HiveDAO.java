package org.dqc.check.dao;

import org.dqc.check.util.ConfigLoader;
import org.dqc.check.util.DefaultConfigLoader;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

public class HiveDAO {

    private static final Logger logger = Logger.getLogger(MysqlDAO.class);

    private DefaultConfigLoader cfg;
    public HiveDAO() throws ClassNotFoundException
            , NoSuchMethodException, InstantiationException, IllegalAccessException
            , InvocationTargetException {
        this.cfg=new ConfigLoader().getConfigInfo();
    }

    public Map<String, StringBuilder> downloadCheck(Map appsmap, String tablename, String ds) {

        //Map<String,String>[] arrmap=new HashMap[2];
        //Map<String,String> map=new HashMap<String, String>();
        //Map<String,String> maperr=new HashMap<String, String>();
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        tablename=this.cfg.properties.getProperty(tablename).split(",")[0];
        String sql=" SELECT appid,count(1) rowss from "+tablename+" where ds='"+ds+"' group by appid";

        Map<String, StringBuilder> sbmap=new HashMap<String, StringBuilder>();
        StringBuilder title = new StringBuilder();
        StringBuilder content = new StringBuilder();
        try{
            conn = this.cfg.getUtils().getHiveTkioConnection();
            logger.info(sql);
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            Integer checkrstkey=0;
            while (rs.next()) {
                String appid = rs.getString("appid");
                String rowss = rs.getString("rowss");
                if (appsmap.containsKey(appid)) {
                    //logger.info("[" + appid + "|" + rowss + "row|" + appsmap.get(appid) + "byte");
                } else {
                    checkrstkey++;
                    if (checkrstkey == 1) {
                        content.append("<b>以下是缺失的APP: </b><br />");
                        content.append("<=========================================><br />");
                        content.append("<table border=\"1\" cellspacing=\"0\">");
                        content.append("<tr>");
                        content.append("<th>APPID</th>");
                        content.append("<th>数据条数</th>");
                        content.append("</tr>");
                    }
                    content.append("<tr>");
                    content.append("<td>" + appid + "</td>");
                    content.append("<td>" + rowss + "</td>");
                    content.append("</tr>");
                    //logger.info("[" + appid + "|" + rowss + "row");
                }
            }
            if (checkrstkey > 0) {
                content.append("</table>");
                title.append("***" + ds + "日" + tablename + "下载功能异常，影响APP个数" + checkrstkey + "，请及时处理***");
            }
        }catch(Exception ex){
            logger.error(ex.toString());
            title.setLength(0);//清空
            title.append("***"+ds+"日"+tablename+"下载功能校验出错");
            content.setLength(0);//清空
            content.append(ex.toString());
        }finally {
            this.cfg.getUtils().release(rs, pstmt, conn);
        }
        sbmap.put("title",title);
        sbmap.put("content",content);
        return sbmap;
    }
}

