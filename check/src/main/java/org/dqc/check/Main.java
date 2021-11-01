package org.dqc.check;

import org.dqc.check.dao.MysqlDAO;
import org.dqc.check.dao.WarningDAO;
import org.dqc.check.thread.ExecThread;
import org.apache.commons.cli.*;
import org.apache.commons.cli.CommandLineParser;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

public class Main {

    private static final Logger logger = Logger.getLogger(Main.class);

    public void execAll(String ds, String taskno, String dsdel) throws ClassNotFoundException
            , NoSuchMethodException, InstantiationException, IllegalAccessException
            , InvocationTargetException, InterruptedException {

        //第一遍
        new ExecThread().execCheck(10,ds,taskno,true);
        //如果第一遍报错，则重跑一次
        new ExecThread().execCheck(1,ds,taskno,false);
        //分指标分app统计差异，跑第一遍
        new ExecThread().execCheckIndicator(5,ds,true);
        //分指标分app统计差异，跑第二遍
        new ExecThread().execCheckIndicator(1,ds,false);
        //汇总结果，按过滤规则对差异数据做过滤
        new MysqlDAO().insertCheckRstIndicator2(ds);

        //发送告警邮件
        String title ="";
        String content="";
        MysqlDAO dao =new MysqlDAO();
        Map<String, StringBuilder> sbmapw=dao.getWarningRst(ds);
        if(sbmapw.get("title").toString().equals("")){
            Map<String, StringBuilder> sbmapr=dao.getCheckRst(ds);
            title=sbmapr.get("title").toString();
            content=sbmapr.get("content").toString();
        }else{
            title=sbmapw.get("title").toString();
            content=sbmapw.get("content").toString();
            try {
                //短信通知

                //打电话报警(影响app数超过10，且问题指标数据超过2个才打电话)

            }
            catch (Exception e1) {
                e1.printStackTrace();
            }
        }
        logger.info(title);
        logger.info(content);
        //MailUtils mailUtils = new MailUtils(new WarningDAO().cfg.properties);
        //mailUtils.sendMailContentEvery(title,content);

        //检查数据校验任务执行-是否出错
        title ="";
        content="";
        sbmapw=dao.getExecCheckTaskRst(ds);
        if(sbmapw.get("title").toString().equals("")){
            logger.info("检查数据校验任务执行-true");
        }else{
            title=sbmapw.get("title").toString();
            content=sbmapw.get("content").toString();
            //mailUtils.sendMailContentEvery(title,content);
            try {
                //短信通知
                //new HttpClientUtil().sendMsg(title);
                //打电话报警
                //new PhoneMsgInfo().sendMsg(title);
            }
            catch (Exception e1) {
                e1.printStackTrace();
            }
        }
        //检查按指标统计差执行-是否出错
        title ="";
        content="";
        sbmapw=dao.getExecCheckIndicatorRst(ds);
        if(sbmapw.get("title").toString().equals("")){
            logger.info("检查按指标统计差异执行-true");
        }else{
            title=sbmapw.get("title").toString();
            content=sbmapw.get("content").toString();
            //mailUtils.sendMailContentEvery(title,content);
            try {
                //短信通知
                //new HttpClientUtil().sendMsg(title);
                //打电话报警
                //new PhoneMsgInfo().sendMsg(title);
            }
            catch (Exception e1) {
                e1.printStackTrace();
            }
        }

        //下载数据处理检查
        //new ExecThread().execDownloadCheck(4,ds);
        //执行结束后删除10天以前的检查结果数据
        dao.deleteCheckRst(dsdel);
    }

    public void execCheck(String ds, String taskno, String dsdel) throws ClassNotFoundException
            , NoSuchMethodException, InstantiationException, IllegalAccessException
            , InvocationTargetException, InterruptedException {

        //第一遍
        new ExecThread().execCheck(10,ds,taskno,true);
        //如果第一遍报错，则重跑一次
        new ExecThread().execCheck(1,ds,taskno,false);
        //分指标分app统计差异，跑第一遍
        new ExecThread().execCheckIndicator(5,ds,true);
        //分指标分app统计差异，跑第二遍
        new ExecThread().execCheckIndicator(1,ds,false);
        //汇总结果，按过滤规则对差异数据做过滤
        new MysqlDAO().insertCheckRstIndicator2(ds);

        //发送告警邮件
        String title ="";
        String content="";
        MysqlDAO dao =new MysqlDAO();
        Map<String, StringBuilder> sbmapw=dao.getWarningRst(ds);
        if(sbmapw.get("title").toString().equals("")){
            Map<String, StringBuilder> sbmapr=dao.getCheckRst(ds);
            title=sbmapr.get("title").toString();
            content=sbmapr.get("content").toString();
        }else{
            title=sbmapw.get("title").toString();
            content=sbmapw.get("content").toString();
            try {
                //短信通知
                //new HttpClientUtil().sendMsg(title);
                //打电话报警(影响app数超过10，且问题指标数据超过2个才打电话)
            }
            catch (Exception e1) {
                e1.printStackTrace();
            }
        }
        logger.info(title);
        logger.info(content);
        //MailUtils mailUtils = new MailUtils(new WarningDAO().cfg.properties);
        //mailUtils.sendMailContentEvery(title,content);

        //检查数据校验任务执行-是否出错
        title ="";
        content="";
        sbmapw=dao.getExecCheckTaskRst(ds);
        if(sbmapw.get("title").toString().equals("")){
            logger.info("检查数据校验任务执行-true");
        }else{
            title=sbmapw.get("title").toString();
            content=sbmapw.get("content").toString();
            //mailUtils.sendMailContentEvery(title,content);
            try {
                //短信通知
                //new HttpClientUtil().sendMsg(title);
                //打电话报警
                //new PhoneMsgInfo().sendMsg(title);
            }
            catch (Exception e1) {
                e1.printStackTrace();
            }
        }
        //检查按指标统计差执行-是否出错
        title ="";
        content="";
        sbmapw=dao.getExecCheckIndicatorRst(ds);
        if(sbmapw.get("title").toString().equals("")){
            logger.info("检查按指标统计差异执行-true");
        }else{
            title=sbmapw.get("title").toString();
            content=sbmapw.get("content").toString();
            //mailUtils.sendMailContentEvery(title,content);
            try {
                //短信通知
                //new HttpClientUtil().sendMsg(title);
                //打电话报警
                //new PhoneMsgInfo().sendMsg(title);
            }
            catch (Exception e1) {
                e1.printStackTrace();
            }
        }

        //执行结束后删除10天以前的检查结果数据
        dao.deleteCheckRst(dsdel);
    }

    public void execDownload(String ds) throws ClassNotFoundException
            , NoSuchMethodException, InstantiationException, IllegalAccessException
            , InvocationTargetException, InterruptedException {
        //下载数据处理检查
        //new ExecThread().execDownloadCheck(4,ds);
    }

    public static void main(String[] args) throws ParseException {

        PropertyConfigurator.configure("checkconf/log4j.properties");

        SimpleDateFormat dft = new SimpleDateFormat("yyyy-MM-dd");
        Date beginDate = new Date();
        Calendar date = Calendar.getInstance();
        date.setTime(beginDate);
        date.set(Calendar.DATE, date.get(Calendar.DATE) - 1);
        String ds = dft.format(date.getTime());

        Calendar date2 = Calendar.getInstance();
        date2.setTime(beginDate);
        date2.set(Calendar.DATE, date2.get(Calendar.DATE) - 10);
        String dsdel = dft.format(date2.getTime());

        String exectype=null ;
        String taskno=null ;
        Options options = new Options();
        options.addOption("exectype", true, "exectype: [all,check,download]");
        options.addOption("ds", true, "ds format：yyyy-MM-dd");
        options.addOption("taskno", true, "10001,10002,10003");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);

        if (!cmd.hasOption("ds")) {
            ds = dft.format(date.getTime());
        }else{
            ds = cmd.getOptionValue("ds");
        }
        if (!cmd.hasOption("exectype")) {
            //throw new RuntimeException("exectype is required!");
            exectype = "all";
        } else{
            exectype = cmd.getOptionValue("exectype");
        }
        taskno=cmd.getOptionValue("taskno");

        //ds="2018-02-27";
        //exectype="download";
        //taskno="10001";

        Main main =new Main();
        try{
            if(exectype.equals("all")){
                main.execAll(ds,taskno,dsdel);
            }else if (exectype.equals("check")){
                main.execCheck(ds,taskno,dsdel);
            }else if (exectype.equals("download")){
                main.execDownload(ds);
            }
        }catch (Exception e){
            logger.info(e.toString());
        }

    }
}
