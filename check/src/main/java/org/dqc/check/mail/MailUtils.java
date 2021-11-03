package org.dqc.check.mail;

import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;

//import org.json.JSONObject;

public class MailUtils {

    private static final Logger logger = Logger.getLogger(MailUtils.class);

    private SimpleMailSender sms;
    private String title;
    private Set<String> ids;
    private String ds;
    String level;
    String iscum;
    // 注意以下方法只适合于crontab环境下使用
    public static HashMap<String, String> mailContent = new HashMap<String, String>();

    public MailUtils(Properties properties) {
        sms = new SimpleMailSender(properties);
    }

    public static synchronized void addMailContent(String key, String value) {
        mailContent.put(key, value);
    }

    public void sendMailContentEvery(String title,String content) {
        sms.sendMail(title,content);
    }
    public void sendMailContent() {
        if (mailContent.size() > 0) { // 如果有报错的情况，才发送邮件
            String subject ="ETL Exceptions For --->have data is:"+iscum+"--->level is:" + level +"--->" + this.title + " At " + this.ds;
            StringBuilder sb = new StringBuilder();
            sb.append("<b>Please check the exceptions below and correct them as soon as possible: </b><br />");
            for (Entry<String, String> entry : mailContent.entrySet()) {
                sb.append(entry.getKey().replace("\n", "<br />") + "<br />");
                sb.append("<=========================================><br />");
                sb.append("<b>" + entry.getValue() + "</b><br /><p />");
            }
            sms.sendMail(subject, sb.toString());
        }
    }

    // 注意以上方法只适合于crontab环境下使用

    public void sendMailStart() throws ClassNotFoundException, SQLException {
        List<String> todayAppIds = new ArrayList<String>(this.ids);

        StringBuffer sb = new StringBuffer();
        sb.append(">====");
        for (String account : todayAppIds) {
            sb.append(account).append("==");
        }
        sb.append("==<");
        sms.sendMail("ETL start ---> have data is:"+iscum+"--->level is:" + level +"--->"+ title + ", DS is: " + this.ds,
                String.format("<b>ETL Start With %d bucketIds</b>", todayAppIds.size())
                        + ", running bucketIds are: " + sb.toString());
    }

/***

    public void sendMailEnd(String isalert) {
        sms.sendMail("ETL end ---> have data is:"+iscum+"--->level is:" + level +"--->" + title + ", DS is: " + this.ds, "<b>ETL END</b>");
        //跑完所有ETL任务，短信通知

        try{
            if("true".equals(isalert)){
                String url = "http://www.linkedsee.com/alarm/cloudchannel";
                JSONObject json = new JSONObject();
                json.put("receiver","q20170912190819330");
                json.put("type", "sms");
                json.put("title", "notice_sms");
                json.put("content", "trackingio ETL 运行结束!  " + ", DS is: " + this.ds +"  ,  and title is: "+ title.toString());
                HttpClientUtil.httpPostWithJson(json ,url ,"Servicetoken");
            }
        }catch (Exception ex){
            ex.printStackTrace();
        }

    }
*/
    public void setlevel(String level) {
        this.level = level;
    }
    public void setiscum(String iscum) {
        this.iscum = iscum;
    }
}
