package org.dqc.check.dao;

public class TaskConfigDAO {

    public Integer ROW_ID     ;
    public String TASK_NO     ;
    public String TASK_DESC   ;
    public String TABLE_TYPE  ;
    public String TABLE_A     ;
    public String TABLE_B     ;
    public String FILTER_WHERE  ;
    public String PARAM_DESC  ;
    public String DIM_NAME    ;
    public String FACT_NAME   ;
    public String DIM_DESC    ;
    public String FACT_DESC   ;

    public void setROW_ID    (Integer ROW_ID     ){this.ROW_ID    = ROW_ID    ; }

    public void setTASK_NO   (String TASK_NO     ){ this.TASK_NO=TASK_NO      ; }
    public void setTASK_DESC (String TASK_DESC  ){this.TASK_DESC = TASK_DESC ; }
    public void setTABLE_TYPE(String TABLE_TYPE ){this.TABLE_TYPE= TABLE_TYPE; }
    public void setTABLE_A   (String TABLE_A    ){this.TABLE_A   = TABLE_A   ; }
    public void setTABLE_B   (String TABLE_B    ){this.TABLE_B   = TABLE_B   ; }
    public void setFILTER_WHERE(String FILTER_WHERE ){this.FILTER_WHERE= FILTER_WHERE; }
    public void setPARAM_DESC(String PARAM_DESC ){this.PARAM_DESC= PARAM_DESC; }
    public void setDIM_NAME  (String DIM_NAME   ){this.DIM_NAME  = DIM_NAME  ; }
    public void setFACT_NAME (String FACT_NAME  ){this.FACT_NAME = FACT_NAME ; }
    public void setDIM_DESC  (String DIM_DESC   ){this.DIM_DESC  = DIM_DESC  ; }
    public void setFACT_DESC (String FACT_DESC  ){this.FACT_DESC = FACT_DESC ; }

    public Integer getROW_ID    (){ return this.ROW_ID    ; }
    public String getTASK_NO   (){ return this.TASK_NO   ; }
    public String getTASK_DESC (){ return this.TASK_DESC ; }
    public String getTABLE_TYPE(){ return this.TABLE_TYPE; }
    public String getTABLE_A   (){ return this.TABLE_A   ; }
    public String getTABLE_B   (){ return this.TABLE_B   ; }
    public String getPARAM_NAME(){ return this.FILTER_WHERE; }
    public String getPARAM_DESC(){ return this.PARAM_DESC; }
    public String getDIM_NAME  (){ return this.DIM_NAME  ; }
    public String getFACT_NAME (){ return this.FACT_NAME ; }
    public String getDIM_DESC  (){ return this.DIM_DESC  ; }
    public String getFACT_DESC (){ return this.FACT_DESC ; }





}
