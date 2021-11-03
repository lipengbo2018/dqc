package org.dqc.check;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.lang.reflect.InvocationTargetException;

/**
 * Hello world!
 *
 */
public class App 
{
    private static final Logger logger = Logger.getLogger(Main.class);

    public static void main( String[] args )throws ClassNotFoundException
            , NoSuchMethodException, InstantiationException, IllegalAccessException
            , InvocationTargetException
    {
        PropertyConfigurator.configure("log4j.properties");

        try {
            //短信通知
            //new HttpClientUtil().sendMsg("error");
            //打电话报警
            //new PhoneMsgInfo().sendMsg("error");
        }
        catch (Exception e1) {
        e1.printStackTrace();
        }

        /**

        //ds,task_no,exec_sql,exec_state,error_info
        String[] logarr=new String[5];
        logarr[0]="9999-01-01";
        logarr[1]="test";
        logarr[2]="SQL语句";
        logarr[3]="0";
        logarr[4]="测试中文乱码";
        MysqlDAO dao =new MysqlDAO();
        dao.insertCheckLog(logarr);

        String ds="2018-03-01";
        //发送告警邮件
        String title ;
        String content ;
        MysqlDAO dao =new MysqlDAO();
        Map<String,StringBuilder> sbmapw;
        MailUtils mailUtils = new MailUtils(new WarningDAO().cfg.properties);
        //检查数据校验任务执行-是否出错
        title ="";
        content="";
        sbmapw=dao.getExecCheckIndicatorRst(ds);
        if(sbmapw.get("title").toString().equals("")){
            logger.info("检查数据校验任务执行-true");
        }else{
            title=sbmapw.get("title").toString();
            content=sbmapw.get("content").toString();
            mailUtils.sendMailContentEvery(title,content);

        }
         */
    }
}
