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

    public void execCheck(Integer threads, String ds, String taskno, Boolean isfirstexec){

        ExecutorService exec = Executors.newFixedThreadPool(threads);

        CreationSQL creationsql=new CreationSQL(ds,taskno,isfirstexec);

        ArrayList<String[]> sqllist=creationsql.creationSQLList();
        for (String[] paramarr : sqllist) {
            Runnable run = new ExecCheck(paramarr);
            exec.execute(run);
        }
        exec.shutdown();
        try {
            exec.awaitTermination(12, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}
