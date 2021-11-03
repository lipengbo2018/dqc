package org.dqc.check.util;

import org.dqc.check.dao.MysqlDAO;
import org.dqc.check.dao.TaskConfigDAO;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class CreationSQL{

    private static final Logger logger = Logger.getLogger(CreationSQL.class);
    private ArrayList<TaskConfigDAO> taskconfiglist ;
    private ArrayList<String[]> sqllist =new ArrayList<String[]>();
    private String ds;

    public CreationSQL(String ds, String tasknolist, Boolean isfirstexec) {
        logger.info(tasknolist);
        if(tasknolist==null){
            this.taskconfiglist=new MysqlDAO().getTaskConfig(ds,isfirstexec);
        }else{
            this.taskconfiglist=new MysqlDAO().getTaskConfigByTasknolist(ds,tasknolist,isfirstexec);
        }
        this.ds=ds;
    }

    public ArrayList<String[]> creationSQLList(){

        for(TaskConfigDAO taskconfig:this.taskconfiglist){
            String[] paramarr=new String[3];
            paramarr[0]=taskconfig.TASK_NO;
            paramarr[1]=this.ds;
            //日志信息
            //ds,task_no,exec_sql,exec_state,error_info
            String[] logarr=new String[5];
            logarr[0]=paramarr[1];
            logarr[1]=paramarr[0];
            logarr[2]=null;
            logarr[3]="0";
            logarr[4]=null;

            String fromsql=null;
            if(taskconfig.TABLE_TYPE.equals("T")){
                fromsql="from "
                        +"\n "+"("
                        +"\n select 'table_a' tname,"+taskconfig.DIM_NAME.replace("|",",")
                        +"\n ,"+taskconfig.FACT_NAME.replace("|",",")
                        +"\n from "+taskconfig.TABLE_A
                        +"\n where ds='"+this.ds+"' "+ taskconfig.FILTER_WHERE
                        +"\n union all"
                        +"\n select 'table_b' tname,"+taskconfig.DIM_NAME.replace("|",",")
                        +"\n ,"+taskconfig.FACT_NAME.replace("|",",")
                        +"\n from "+taskconfig.TABLE_B
                        +"\n where ds='"+this.ds+"' "+ taskconfig.FILTER_WHERE
                        +"\n ) t";
            }else if (taskconfig.TABLE_TYPE.equals("V")){
                fromsql="from "
                        +"\n "+"("
                        +"\n "+taskconfig.TABLE_A.replace("${ds}",ds)
                        +"\n union all"
                        +"\n "+taskconfig.TABLE_B.replace("${ds}",ds)
                        +"\n ) t";
            }else {
                logarr[3]="-1";
                logarr[4]="t_sys_task_config配置信息错误：table_type字段只能是T或V";
                logger.error("t_sys_task_config配置信息错误：table_type字段只能是T或V");
            }
            if(taskconfig.FACT_NAME.split("\\|").length != taskconfig.FACT_DESC.split("\\|").length){
                logarr[3]="-1";
                logarr[4]="t_sys_task_config配置信息错误：dim_desc配置的指标个数与dim_name不一致";
                logger.error("t_sys_task_config配置信息错误：dim_desc配置的指标个数与dim_name不一致");
            }
            if (logarr[3].equals("-1")){
                try{
                    new MysqlDAO().insertCheckLog(logarr);
                }catch (Exception e){
                    logger.error(e.toString());
                }
            }else{
                paramarr[2]=this.creationSQL(taskconfig,fromsql);
                this.sqllist.add(paramarr);
            }
        }
        return this.sqllist;
    }

    public String creationSQL(TaskConfigDAO taskconfig, String fromsql){
        String sql=null;
        String selectsql=null;
        String groupbysql=null;

        selectsql=" select '"+this.ds+"' ds"
                +"\n ,'"+taskconfig.TASK_NO+"' task_no"
                +"\n ,'"+taskconfig.FACT_DESC+"' fact_desc"
                +"\n ,'"+taskconfig.DIM_NAME+"' dim_name";
        String[] dimarr = taskconfig.DIM_NAME.split("\\|");
        String dimsql="\n ,concat(";
        for(int i = 0;i<dimarr.length;i++){
            if (i==0){
                dimsql=dimsql+dimarr[i];
            }else {
                dimsql=dimsql+",'|',"+dimarr[i];
            }
        }
        selectsql=selectsql+dimsql+") dim_list";
        String[] factarr = taskconfig.FACT_NAME.split("\\|");
        String[] factdescarr = taskconfig.FACT_DESC.split("\\|");
        String factsql="\n ,concat(";
        for(int i = 0;i<factarr.length;i++){
            if (i==0) {
                factsql=factsql
                        +"\n         if(sum(if(tname='table_a',"+factarr[i]+",0))<>sum(if(tname='table_b',"+factarr[i]+",0))"
                        +"\n           ,concat('["+factdescarr[i]+":',"+"sum(if(tname='table_a',"+factarr[i]+",0))"+",'|',sum(if(tname='table_b',"+factarr[i]+",0))  ,']')"
                        +"\n           ,'')";
            }else {
                factsql=factsql
                        +"\n         ,if(sum(if(tname='table_a',"+factarr[i]+",0))<>sum(if(tname='table_b',"+factarr[i]+",0))"
                        +"\n           ,concat(';[','"+factdescarr[i]+":',"+"sum(if(tname='table_a',"+factarr[i]+",0))"+",'|',sum(if(tname='table_b',"+factarr[i]+",0))  ,']')"
                        +"\n           ,'')";
            }
        }
        selectsql=selectsql+factsql+"\n) fact_list";

        selectsql=selectsql+"\n"+fromsql;
        groupbysql="group by "+taskconfig.DIM_NAME.replace("|",",");
        sql=selectsql+"\n"+groupbysql;
        //sql= "\ninsert into t_sys_data_check_result(param_value,task_no,dim_list,fact_list)"
        sql="\nselect ds,task_no,dim_list" +
                ",if(substr(fact_list,1,1)=';',substr(fact_list,2),fact_list) fact_list" +
                ",fact_desc,dim_name,now() create_time"
            +"\nfrom"
            +"\n("
            +"\n"+sql
            +"\n) t";
            //+"\nwhere fact_list <> ''";
        return sql;
    }

}
