SET DEFINE OFF;
--drop table DS_job_log;

CREATE TABLE DS_JOB_LOG
(
  JOB_LOG_ID        NUMBER              NOT NULL,
  JOB_ID            NUMBER              NOT NULL,
  JOB_NAME          VARCHAR2(255 BYTE)  NOT NULL,
  LOG_TYPE          VARCHAR2(20)        DEFAULT 'INFO' NOT NULL,
  STATUS_MESSAGE    VARCHAR2(200 BYTE),   
  PACKAGE_NAME      VARCHAR2(30 BYTE),
  PROCEDURE_NAME    VARCHAR2(255 BYTE),
  ROUTINE_NAME      VARCHAR2(100 BYTE),
  CREATE_DATE       DATE                DEFAULT SYSDATE NOT NULL,
  CREATE_USER_ID    VARCHAR2(30 BYTE)   DEFAULT USER    NOT NULL,
  UPDATE_DATE       DATE,
  UPDATE_USER_ID    VARCHAR2(30 BYTE),
  ERROR_CODE        VARCHAR2(255 BYTE),
  ERROR_MESSAGE     VARCHAR2(4000),
  STATUS_MESSAGE1   VARCHAR2(4000 BYTE),
  STATUS_MESSAGE2   VARCHAR2(4000 BYTE),
  STATUS_MESSAGE3   VARCHAR2(4000 BYTE),
  STATUS_message4   VARCHAR2(4000 BYTE),
  CALL_STACK        VARCHAR2(2000 BYTE),
  ERROR_STACK       VARCHAR2(2000 BYTE),
  ERROR_BACKTRACE   VARCHAR2(2000 BYTE),
  SESSION_INFO      VARCHAR2(2000 BYTE)
)
NOPARALLEL
MONITORING;


CREATE UNIQUE INDEX DS_JOB_LOG_PK ON DS_JOB_LOG
(JOB_LOG_ID)
NOPARALLEL;

CREATE SEQUENCE DS_JOB_LOG_SEQ
  START WITH 1001
  MAXVALUE 999999999999999999999999999
  MINVALUE 1
  NOCYCLE
  CACHE 1000
  NOORDER;


CREATE SEQUENCE DS_JOB_ID_SEQ
  START WITH 1001
  MAXVALUE 999999999999999999999999999
  MINVALUE 1
  NOCYCLE
  CACHE 1000
  NOORDER;


DROP trigger DS_JOB_LOG_TIU;
CREATE OR REPLACE TRIGGER DS_JOB_LOG_TIU
BEFORE INSERT OR UPDATE ON DS_JOB_LOG
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
DECLARE
    tmpVar NUMBER;
BEGIN
    IF INSERTING THEN
        IF :NEW.JOB_LOG_ID is null THEN
           :NEW.JOB_LOG_ID := DS_JOB_LOG_SEQ.NEXTVAL;
        END IF;
        IF :NEW.JOB_ID is null THEN
           :NEW.JOB_ID := DS_JOB_ID_SEQ.NEXTVAL;
        END IF;        
        :NEW.CREATE_DATE     := SYSDATE;
        :NEW.CREATE_USER_ID  := USER;
        :NEW.LOG_TYPE        := UPPER( :NEW.LOG_TYPE ); 
        IF :NEW.LOG_TYPE NOT IN ('BEGIN','INFO','DEBUG','WARNING','ERROR','END' ) THEN :NEW.LOG_TYPE := 'INFO'; END IF;
    ELSE
        :NEW.UPDATE_DATE    := SYSDATE;
        :NEW.UPDATE_USER_ID := USER;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE;
END;
/

ALTER TABLE DS_JOB_LOG DROP CONSTRAINT DS_JOB_LOG_C01;
ALTER TABLE DS_JOB_LOG ADD (
  CONSTRAINT DS_JOB_LOG_C01
  CHECK (log_type IN ('BEGIN','INFO','DEBUG','WARNING','ERROR','END'))
  ENABLE VALIDATE);
