SET DEFINE OFF;
CREATE OR REPLACE PACKAGE             db_logging_pkg IS


   PROCEDURE Log(pJobName         IN VARCHAR2 DEFAULT NULL
                ,pLogType         IN VARCHAR2 DEFAULT 'Info'
                ,pStatusMessage   IN VARCHAR2 DEFAULT NULL
                ,pPackageName     IN VARCHAR2 DEFAULT NULL
                ,pProcedureName   IN VARCHAR2 DEFAULT NULL
                ,pRoutineName     IN VARCHAR2 DEFAULT NULL
                ,pErrorCode       IN VARCHAR2 DEFAULT NULL
                ,pErrormessage    IN VARCHAR2 DEFAULT NULL
                ,pStatusMessage1  IN VARCHAR2 DEFAULT NULL
                ,pStatusMessage2  IN VARCHAR2 DEFAULT NULL
                ,pStatusMessage3  IN VARCHAR2 DEFAULT NULL
                ,pStatusMessage4  IN VARCHAR2 DEFAULT NULL
                );

/*
  Valid values for log procedure parameter pLogType are 'BEGIN','INFO','DEBUG','WARNING','ERROR','END'
*/

END db_logging_pkg;

/



SET DEFINE OFF;
CREATE OR REPLACE PACKAGE BODY  db_logging_pkg IS

         gJobId           ds_job_log.job_id%TYPE         := NULL;
         gJobName         ds_job_log.job_name%TYPE       := NULL;
         gPackageName     ds_job_log.package_name%TYPE   := NULL;
         gProcedureName   ds_job_log.procedure_name%TYPE := NULL;


   PROCEDURE Log(pJobName         IN VARCHAR2 DEFAULT NULL
                ,pLogType         IN VARCHAR2 DEFAULT 'Info'
                ,pStatusMessage   IN VARCHAR2 DEFAULT NULL
                ,pPackageName     IN VARCHAR2 DEFAULT NULL
                ,pProcedureName   IN VARCHAR2 DEFAULT NULL
                ,pRoutineName     IN VARCHAR2 DEFAULT NULL
                ,pErrorCode       IN VARCHAR2 DEFAULT NULL
                ,pErrorMessage    IN VARCHAR2 DEFAULT NULL
                ,pStatusMessage1  IN VARCHAR2 DEFAULT NULL
                ,pStatusMessage2  IN VARCHAR2 DEFAULT NULL
                ,pStatusMessage3  IN VARCHAR2 DEFAULT NULL
                ,pStatusMessage4  IN VARCHAR2 DEFAULT NULL
                )
    IS
      lCallStack        VARCHAR2 (2000) := '';
      lErrorStack       VARCHAR2 (2000) := '';
      lBacktraceStack   VARCHAR2 (2000) := '';
      lSessionInfo      VARCHAR2 (2000) := '';

    BEGIN

       gJobId         :=  NVL( gJobId        , DS_JOB_ID_SEQ.NEXTVAL );
       gJobName       :=  NVL( pJobName      , gJobName      );
       gPackageName   :=  NVL( pPackageName  , gPackageName  );
       gProcedureName :=  NVL( pProcedureName, gProcedureName);

       IF pErrorCode IS NOT NULL OR
          pLogtype   = 'ERROR'
       THEN
           lCallStack      := SUBSTR( DBMS_UTILITY.format_call_stack     ,1,2000 );
           lErrorStack     := SUBSTR( DBMS_UTILITY.format_error_stack    ,1,2000 );
           lBacktraceStack := SUBSTR( DBMS_UTILITY.format_error_backtrace,1,2000 );

           DBMS_OUTPUT.PUT_LINE( 'Call Stack::'      || lCallStack      );
           DBMS_OUTPUT.PUT_LINE( 'Error Stack::'     || lErrorStack     );
           DBMS_OUTPUT.PUT_LINE( 'Backtrace Stack::' || lBacktraceStack );

           lSessionInfo := 'SESSION_USER::' ||SUBSTR( SYS_CONTEXT ('USERENV', 'SESSION_USER') ,1,40) ||CHR(10)
                         ||'SESSIONID::'    ||SUBSTR( SYS_CONTEXT ('USERENV', 'SESSIONID')    ,1,40) ||CHR(10)
                         ||'SID::'          ||SUBSTR( SYS_CONTEXT ('USERENV', 'SID')          ,1,40) ||CHR(10)
                         ||'HOST::'         ||SUBSTR( SYS_CONTEXT ('USERENV', 'HOST')         ,1,40) ||CHR(10)
                         ||'INSTANCE_NAME::'||SUBSTR( SYS_CONTEXT ('USERENV', 'INSTANCE_NAME'),1,40) ||CHR(10)
                         ||'MODULE::'       ||SUBSTR( SYS_CONTEXT ('USERENV', 'MODULE')       ,1,40) ||CHR(10)
                         ||'OS_USER::'      ||SUBSTR( SYS_CONTEXT ('USERENV', 'OS_USER')      ,1,40) ||CHR(10)
                         ||'TERMINAL::'     ||SUBSTR( SYS_CONTEXT ('USERENV', 'TERMINAL')     ,1,40);

       END IF;


    INSERT INTO ds_job_log
            (JOB_ID
            ,JOB_NAME
            ,LOG_TYPE
            ,STATUS_MESSAGE
            ,PACKAGE_NAME
            ,PROCEDURE_NAME
            ,ROUTINE_NAME
            ,ERROR_CODE
            ,ERROR_MESSAGE
            ,STATUS_MESSAGE1
            ,STATUS_MESSAGE2
            ,STATUS_MESSAGE3
            ,STATUS_MESSAGE4
            ,CALL_STACK
            ,ERROR_STACK
            ,ERROR_BACKTRACE
            ,SESSION_INFO)
    VALUES  (gJobId
            ,gJobName
            ,pLogType
            ,pStatusMessage
            ,gPackageName
            ,gProcedureName
            ,pRoutineName
            ,pErrorCode
            ,pErrormessage
            ,SUBSTR( pStatusMessage1,1,4000)
            ,SUBSTR( pStatusMessage2,1,4000)
            ,SUBSTR( pStatusMessage3,1,4000)
            ,SUBSTR( pStatusMessage4,1,4000)
            ,lCallStack
            ,lErrorStack
            ,lBacktraceStack
            ,lSessionInfo
            );

      COMMIT;

   EXCEPTION
   WHEN OTHERS
   THEN
       /*
        To avoid endless loop, skip calling the log procedure when logging errors generated by the db_logging_pkg
       */
       IF pPackageName <> 'DB_LOGGING_PKG'
       THEN Log(pJobName        => 'LOGGING ERROR'
               ,pLogType        => 'ERROR'
               ,pStatusMessage  => 'Error while logging for '|| gJobName
               ,pPackageName    => 'DB_LOGGING_PKG'
               ,pProcedureName  => 'LOG'
               ,pErrorMessage   =>  'gJobId'        => gJobId         ||CHR(10)
                                    'gJobName'      => gJobName       ||CHR(10)
                                    'gPackageName'  => gPackageName   ||CHR(10)
                                    'gProcedureName'=> gProcedureName
                );

            COMMIT;

      END IF;

      RAISE;

   END;

END db_logging_pkg;