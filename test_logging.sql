
DECLARE
      lErrCode  NUMBER;
      lErrMsg   VARCHAR2 (500);
      lCtr      NUMBER;
 BEGIN

      db_logging_pkg.log(pJobName=>'Test Job',pLogType=>'BEGIN',pPackageName=>'MyTestPkg',pProcedureName=>'TestProc');
      
      db_logging_pkg.log(pStatusMessage=>'Do some work');
      
      db_logging_pkg.log( pLogType=>'END');
      
  EXCEPTION
   WHEN OTHERS
   THEN
        lErrCode        := SQLCODE;
        lErrMsg         := SQLERRM;

        db_logging_pkg.log( pLogType       =>'ERROR'
                           ,pErrorCode     => lErrCode
                           ,pErrormessage  =>'Error Msg::'||lErrMsg                         
                           );

        DBMS_OUTPUT.PUT_LINE( 'Error Code::' || lErrCode       );
        DBMS_OUTPUT.PUT_LINE( 'Error Msg::'  || lErrMsg        );       

        RAISE;
   END;      
/
   


DECLARE
      lErrCode  NUMBER;
      lErrMsg   VARCHAR2 (500);
      lCtr      NUMBER;
 BEGIN

      db_logging_pkg.log(pJobName=>'Test Job2',pLogType=>'BEGIN',pPackageName=>'MyTestPkg',pProcedureName=>'TestProc');
      
      db_logging_pkg.log(pStatusMessage=>'Do some work');
      
      lCtr := 1/0;
      
      db_logging_pkg.log( pLogType=>'END');
      
  EXCEPTION
   WHEN OTHERS
   THEN
        lErrCode        := SQLCODE;
        lErrMsg         := SQLERRM;

        db_logging_pkg.log( pLogType       =>'ERROR'
                           ,pErrorCode     => lErrCode
                           ,pErrormessage  =>'Error Msg::'||lErrMsg                         
                           );

        DBMS_OUTPUT.PUT_LINE( 'Error Code::' || lErrCode       );
        DBMS_OUTPUT.PUT_LINE( 'Error Msg::'  || lErrMsg        );       

        RAISE;
   END;      
/
   


select *
  from ds_job_log
/