package org.dqc.check.thread;

import org.dqc.check.dao.MysqlDAO;
import org.dqc.check.util.CreationSQL;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.*;

public class ExecThread {

    private static final Logger logger = Logger.getLogger(ExecThread.class);

    public void execCheck(Integer threads, String ds, String taskno, Boolean isfirstexec) throws ClassNotFoundException
            , NoSuchMethodException, InstantiationException, IllegalAccessException
            , InvocationTargetException, InterruptedException {

        ExecutorService exec = Executors.newFixedThreadPool(threads);

        CreationSQL creationsql=new CreationSQL(ds,taskno,isfirstexec);
        ArrayList<String[]> sqllist=creationsql.creationSQLList();
        for (String[] paramarr : sqllist) {
            Runnable run = new ExecCheck(paramarr);
            exec.execute(run);
        }
        exec.shutdown();
        exec.awaitTermination(12, TimeUnit.HOURS);

    }

    public void execCheckIndicator(Integer threads, String ds, Boolean isfirstexec) throws ClassNotFoundException
            , NoSuchMethodException, InstantiationException, IllegalAccessException
            , InvocationTargetException, InterruptedException {
        //分指标统计差异，并按过滤规则对差异数据做过滤

        ArrayList<String> indicatorlist=new MysqlDAO().getIndicatorInfo(ds,isfirstexec);
        ExecutorService execidct = Executors.newFixedThreadPool(threads);
        for (String indicator : indicatorlist) {
            Runnable run = new ExecCheckIndicator(ds,indicator);
            execidct.execute(run);
        }
        execidct.shutdown();
        execidct.awaitTermination(12, TimeUnit.HOURS);
    }

}
