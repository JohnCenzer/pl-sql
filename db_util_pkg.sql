
CREATE OR REPLACE PACKAGE db_util_pkg
AUTHID CURRENT_USER
AS

/****************************************************************
* Script Name: db_util_pkg.sql
*
* Date         Author     Description
* 20-Jul-2015  JCenzer    Create a package for commonly used DB operations.
*
****************************************************************/

PROCEDURE bulkBackup(pSrcTableName IN VARCHAR2,
                     pBkpTableName IN VARCHAR2,
                     pBatchSize    IN NUMBER   DEFAULT 10000,
                     pWhereClause  IN VARCHAR2 DEFAULT NULL,
                     pDeleteSrc    IN VARCHAR2 DEFAULT 'N');


PROCEDURE bulkDelete(pSrcTableName IN VARCHAR2,
                     pBatchSize    IN NUMBER   DEFAULT 10000,
                     pWhereClause  IN VARCHAR2 DEFAULT NULL);

FUNCTION elapsedTime(pStartDate IN DATE,
                     pEndDate   IN DATE)
         RETURN VARCHAR2;

PROCEDURE Timer(pMessage IN VARCHAR2 DEFAULT 'START');


PROCEDURE rebuildIndexPrc( pTableName         IN VARCHAR2
                          ,pIndexName         IN VARCHAR2 DEFAULT NULL
                          ,pGatherStats       IN VARCHAR2 DEFAULT 'Y'
                          ,pInvalidOnly       IN VARCHAR2 DEFAULT 'Y'
                          ,pRebuildOption     IN VARCHAR2 DEFAULT 'PARALLEL(DEGREE 32) NOLOGGING'
                          ,pPostRebuildParams IN VARCHAR2 DEFAULT 'CURRENT'
                          ,pOnline            IN VARCHAR2 DEFAULT 'Y'
                          ,pTableSpace        IN VARCHAR2 DEFAULT NULL
                          ,pPartitionName     IN VARCHAR2 DEFAULT NULL
                          ,pSubPartitionName  IN VARCHAR2 DEFAULT NULL
                          );
--
-- This procedure rebuilds indexes for a specified table.
-- Index statistics are gathered after rebuilding each index.
--
--   pTableName - name of the table to rebuild indexes
--   pIndexName - name of index.
--      Default is NULL which means all indexes on the table
--   pGatherStats - gather stats on each index.
--       Default is 'Y'.
--   pInvalidOnly - Invalid indexes only.
--      'Y' means invalid only, 'N' means all indexes
--   pRebuildOption - options to use for rebuilding indexes.
--      Default is 'PARALLEL(DEGREE 32) NOLOGGING'
--      NULL will use current settings
--   pPostRebuildParams - index parameters will be reset after rebuild.
--      Default is 'CURRENT', which are the current parameters prior to rebuilding.
--   pOnline - use online rebuild or not.
--      Default is 'Y' use online rebuild.
--      'N' means not online
--   pTableSpace - name of tablespace to rebuild index in.
--      Default value of NULL means current tablespace
--   pPartitionName - partition name if only rebuilding a specific index partition
--   pSubPartitionName - subpartition name if only rebuilding a specific index subpartition
--

PROCEDURE moveTablePrc( pTableName         IN VARCHAR2
                       ,pMoveOption        IN VARCHAR2 DEFAULT 'PARALLEL(DEGREE 32) NOLOGGING'
                       ,pRebuildIndexes    IN VARCHAR2 DEFAULT 'Y'
                       ,pPostRebuildParams IN VARCHAR2 DEFAULT 'CURRENT'
                       ,pTableSpace        IN VARCHAR2 DEFAULT NULL
                       ,pPartitionName     IN VARCHAR2 DEFAULT NULL
                       ,pSubPartitionName  IN VARCHAR2 DEFAULT NULL
                      );
--
-- This procedure moves a table and rebuilds its indexes after moving.
-- Index statistics are gathered after rebuilding each index.
--
--   pTableName - name of the table to move
--   pMoveOption - options to use for moving the table.
--      Default is 'PARALLEL(DEGREE 32) NOLOGGING'
--      NULL will use current settings
--   pRebuildIndexes - rebuild indexes after moving table.
--      Default is 'Y'
--   pPostRebuildParams - table parameters will be reset after move.
--      Default is 'CURRENT', which are the current parameters prior to rebuilding.
--   pTableSpace - name of tablespace to move the table.
--      Default value of NULL means current tablespace
--   pPartitionName - partition name if only moving a specific table partition
--   pSubPartitionName - subpartition name if only moving a specific table subpartition
--

END;
/

CREATE OR REPLACE PACKAGE BODY db_util_pkg
AS
/****************************************************************
* Script Name: db_util_pkg.sql
*
* Date         Author     Description
* 20-Jul-2015  JCenzer    Create a package for commonly used DB operations.
****************************************************************/

gStartDate  DATE := TO_DATE(NULL); -- used by timer function


/****************************************************************
* Name: bulkBackup
* Purpose: Copy data from one table to another table in batches
*          using bulk collect.
*          Paremter pDeleteSrc allows deletion of date from source
*          after backing up.
*          pBkpTableName will be created IF it does not exist
****************************************************************/
PROCEDURE bulkBackup(pSrcTableName IN VARCHAR2,
                     pBkpTableName IN VARCHAR2,
                     pBatchSize    IN NUMBER   DEFAULT 10000,
                     pWhereClause  IN VARCHAR2 DEFAULT NULL,
                     pDeleteSrc    IN VARCHAR2 DEFAULT 'N')
AS

lColumnList VARCHAR2(4000);

v_Sql  VARCHAR2(5000);


BEGIN

  /* create backup table IF it does not exist. Need SYS priv - create table
  IF NOT tableExists(pBkpTableName)
  THEN
      EXECUTE IMMEDIATE 'CREATE TABLE '||pBkpTableName||' NOLOGGING AS SELECT * FROM '||pSrcTableName||' WHERE 1=0';
  END IF;
  */

  SELECT 'c_tab(i).'||listagg( column_name, ',c_tab(i).') WITHIN GROUP (ORDER BY column_id)
    INTO lColumnList
    FROM user_tab_columns
   WHERE table_name = pSrcTableName;

v_Sql :='
DECLARE

CURSOR c
IS
SELECT src.rowid AS row_id, src.*
  FROM '||pSrcTableName||' src
 '||pWhereClause||';
TYPE c_tab_type IS TABLE OF c%ROWTYPE;
c_tab c_tab_type;

BEGIN

 OPEN c;

 LOOP
   FETCH c BULK COLLECT INTO c_tab LIMIT '||pBatchSize||';
   /* Exit when no rows collected */
   IF C_TAB.EXISTS(1)=FALSE
   THEN
     EXIT;
   END IF;

   FORALL i IN c_tab.FIRST..c_tab.LAST
   INSERT /*+APPEND*/ INTO '||pBkpTableName||' VALUES ( '||lColumnList||');

   IF '''||pDeleteSrc||''' = ''Y''
   THEN
     FORALL i IN c_tab.FIRST..c_tab.LAST
     DELETE FROM '||pSrcTableName||' WHERE rowid = c_tab(i).row_id;
   END IF;

   COMMIT;

 END LOOP;

END;';

 EXECUTE IMMEDIATE v_sql;

EXCEPTION
WHEN OTHERS THEN
  dbms_output.put_line(v_sql);
  dbms_output.put_line (dbms_utility.format_error_stack ());
  dbms_output.put_line (dbms_utility.format_error_backtrace ());
  RAISE;

END bulkBackup;


/****************************************************************
* Name: bulkDelete
* Purpose: Delete data from a table in batches using bulk collect.
****************************************************************/
PROCEDURE bulkDelete(pSrcTableName IN VARCHAR2,
                     pBatchSize    IN NUMBER   DEFAULT 10000,
                     pWhereClause  IN VARCHAR2 DEFAULT NULL)
AS

v_Sql  VARCHAR2(5000):='
DECLARE

CURSOR c
IS
SELECT rowid AS row_id
  FROM '||pSrcTableName||'
 '||pWhereClause||';
TYPE c_tab_type IS TABLE OF c%ROWTYPE;
c_tab c_tab_type;

BEGIN

 OPEN c;

 LOOP
   FETCH c BULK COLLECT INTO c_tab LIMIT '||pBatchSize||';
   EXIT WHEN c%NOTFOUND;

   FORALL i IN c_tab.FIRST..c_tab.LAST
   DELETE
     FROM '||pSrcTableName||'
    WHERE rowid = c_tab(i).row_id;

   COMMIT;

 END LOOP;

END;';


BEGIN


 EXECUTE IMMEDIATE v_sql;

EXCEPTION
WHEN OTHERS THEN
  dbms_output.put_line(v_sql);
  dbms_output.put_line (dbms_utility.format_error_stack ());
  dbms_output.put_line (dbms_utility.format_error_backtrace ());
  RAISE;
END bulkDelete;


/****************************************************************
* Name: elapsedTime
* Purpose: Returns the dIFference between 2 dates in DD:HH:MI:SS FORmat
****************************************************************/
FUNCTION elapsedTime(pStartDate IN DATE,
                     pEndDate   IN DATE)
         RETURN VARCHAR2
IS
returnValue VARCHAR2(30);

BEGIN

    returnValue := TO_CHAR(EXTRACT(DAY FROM NUMTODSINTERVAL(pEndDate-pStartDate, 'DAY')), 'FM00')
                    || ' ' ||
                 TO_CHAR(EXTRACT(HOUR FROM NUMTODSINTERVAL(pEndDate-pStartDate, 'DAY')), 'FM00')
                    || ':' ||
                 TO_CHAR(EXTRACT(MINUTE FROM NUMTODSINTERVAL(pEndDate-pStartDate, 'DAY')), 'FM00')
                    || ':' ||
                 TO_CHAR(EXTRACT(SECOND FROM NUMTODSINTERVAL(pEndDate-pStartDate, 'DAY')), 'FM00');

  RETURN returnValue;

EXCEPTION
WHEN OTHERS
THEN RETURN NULL;

END elapsedTime;

PROCEDURE Timer(pMessage IN VARCHAR2 DEFAULT 'START')

IS
  returnValue VARCHAR2(30);

BEGIN

  IF gStartDate IS NOT NULL
  THEN
      returnValue := SUBSTR( elapsedTime(pStartDate=> gStartDate,
                                         pEndDate=>SYSDATE), 4, 10); --substr to start at hours
  END IF;

  gStartDate:=SYSDATE;

  dbms_output.put_line( returnValue || ' - ' || pMessage );

END Timer;


PROCEDURE rebuildIndexPrc( pTableName         IN VARCHAR2
                          ,pIndexName         IN VARCHAR2 DEFAULT NULL
                          ,pGatherStats       IN VARCHAR2 DEFAULT 'Y'
                          ,pInvalidOnly       IN VARCHAR2 DEFAULT 'Y'
                          ,pRebuildOption     IN VARCHAR2 DEFAULT 'PARALLEL(DEGREE 32) NOLOGGING'
                          ,pPostRebuildParams IN VARCHAR2 DEFAULT 'CURRENT'
                          ,pOnline            IN VARCHAR2 DEFAULT 'Y'
                          ,pTableSpace        IN VARCHAR2 DEFAULT NULL
                          ,pPartitionName     IN VARCHAR2 DEFAULT NULL
                          ,pSubPartitionName  IN VARCHAR2 DEFAULT NULL
                          )
IS
--

  lStep               VARCHAR2 (100 BYTE);
  lSql                VARCHAR2(2000 BYTE);
  lCtr                PLS_INTEGER         := 0;
  lProcName           VARCHAR2  (30 BYTE) := 'rebuildIndexPrc';
  
  CURSOR ind_cur
  IS SELECT
            it.index_name
           ,itp.partition_name
           ,itsp.subpartition_name
           --
           -- Alter index parameters prior to rebuilding
           -- Only the first row returned for each index will have a value
           --
           ,CASE WHEN ROW_NUMBER() OVER (PARTITION BY it.index_name ORDER BY it.index_name, itp.partition_name, itsp.subpartition_name ) = 1
                 THEN 'ALTER INDEX '|| it.index_name || ' ' || pRebuildOption
             END preBuildOptions
           --
           -- Generate the sql to rebuild the index, partitions and subpartitions if they exist
           --
           ,CASE WHEN itp.partition_name IS NULL
            THEN
                 'ALTER INDEX ' || it.index_name || ' REBUILD '
            WHEN itsp.subpartition_name IS NULL
            THEN
                 'ALTER INDEX ' || it.index_name || ' REBUILD PARTITION '|| itp.partition_name
            ELSE
                 'ALTER INDEX ' || it.index_name || ' REBUILD SUBPARTITION '|| itsp.subpartition_name
             END ||' '
            || CASE WHEN pTableSpace IS NULL THEN NULL ELSE ' TABLESPACE ' || pTableSpace || ' ' END
            || CASE WHEN pOnline <> 'Y' THEN NULL ELSE 'ONLINE' END AS rebuild_stmt
           --
           -- Reset index parameters after rebuilding
           -- Only the last row returned for each index will have a value
           --
           ,CASE WHEN ROW_NUMBER() OVER (PARTITION BY it.index_name ORDER BY it.index_name DESC ,itp.partition_name DESC ,itsp.subpartition_name DESC ) = 1
                 THEN
                 CASE WHEN pPostRebuildParams = 'CURRENT'
                      THEN
                            'ALTER INDEX ' || it.index_name || ' PARALLEL( DEGREE ' || it.degree || ' INSTANCES '|| it.instances ||') '||
                            CASE WHEN NVL( it.logging, itp.logging )  = 'YES' THEN 'LOGGING' ELSE 'NOLOGGING' END
                      ELSE
                           'ALTER INDEX ' || it.index_name || ' ' || pPostRebuildParams
                  END
              END reset_stmt
            --
       FROM user_indexes it
       LEFT JOIN user_ind_partitions itp
         ON it.index_name      = itp.index_name
       --
       LEFT JOIN user_ind_subpartitions itsp
         ON itp.index_name     = itsp.index_name AND
            itp.partition_name = itsp.partition_name
       --
      WHERE it.table_name      = pTableName
        AND ( pIndexName        IS NULL OR
              pIndexName        = it.index_name )
        AND ( pPartitionName    IS NULL OR
              pPartitionName    = itp.index_name )
        AND ( pSubPartitionName IS NULL OR
              pSubPartitionName = itsp.index_name )
        AND ( pInvalidOnly = 'N' OR
              ( it.status        IN ('INVALID', 'UNUSABLE')
                OR
                ( itp.status     IN ('INVALID', 'UNUSABLE') AND
                  it.status      = 'N/A'
                )
                OR
                ( itsp.status    IN ('INVALID', 'UNUSABLE') AND
                  itp.status     = 'N/A'
                )
              )
            )
      ORDER BY
            it.index_name
           ,itp.partition_name
           ,itsp.subpartition_name;


BEGIN

   --
   -- Alter session prior to starting process
   --
   EXECUTE IMMEDIATE 'alter session enable parallel dml';
   EXECUTE IMMEDIATE 'alter session set db_file_multiblock_read_count=128';
   EXECUTE IMMEDIATE 'alter session set ddl_lock_timeout=10';


   FOR ind_rec IN ind_cur
   LOOP

       lCtr := lCtr + 1;

       -- Alter index parameters prior to rebuilding
       -- This value is only populated for the first row returned for each index
       IF ind_rec.preBuildOptions IS NOT NULL
       THEN
           lStep := lProcName || '::' || 'Step 1::Set pre-rebuild index options';
           lSql:=ind_rec.preBuildOptions;
           dbms_output.put_line( lStep || '::'|| lSql ||';' );
           EXECUTE IMMEDIATE lSql;
           dbms_output.put_line('********************');

       END IF;


       lStep := lProcName || '::' || 'Step 2::Rebuild index';
       lSql:=ind_rec.rebuild_stmt;
       dbms_output.put_line( lStep || '::'|| lSql ||';' );
       EXECUTE IMMEDIATE lSql;


       -- This value is only populated for the last row for that index
       IF ind_rec.reset_stmt IS NOT NULL
       THEN
           dbms_output.put_line('********************');
           IF pGatherStats = 'Y'
           THEN
                lStep := lProcName || '::' || 'Step 3::Gather Index Stats for ' || ind_rec.index_name;
                dbms_output.put_line( lStep );
                BEGIN
                DBMS_STATS.GATHER_INDEX_STATS (
                     OwnName            => USER
                    ,IndName            => ind_rec.index_name
                    ,PartName           => ind_rec.partition_name
                    ,Granularity        => 'AUTO'
                    ,Degree             => 32
                    ,No_Invalidate      => FALSE
                    ,force              => TRUE);
                END;
           
           ELSE
                lStep := lProcName || '::' || 'Step 3::Index Stats not gathered for ' || ind_rec.index_name;
                dbms_output.put_line( lStep );
                
           END IF;


           --
           -- Reset index parameters which were changed for the rebuild, generally to NOPARALLEL and LOGGING
           --
           dbms_output.put_line('********************');
           lStep := lProcName || '::' || 'Step 4::Reset Params after rebuild';
           lSql:=ind_rec.reset_stmt;
           dbms_output.put_line( lStep || '::'|| lSql ||';' );
           EXECUTE IMMEDIATE lSql;
           dbms_output.put_line('********************' || CHR(10) );

       END IF;

   END LOOP;


   IF lCtr = 0
   THEN
       IF pInvalidOnly = 'Y'
       THEN
           dbms_output.put_line( 'No INVALID/UNUSABLE indexes found' ) ;

       ELSE
           dbms_output.put_line( 'No indexes found' ) ;

       END IF;

   END IF;


EXCEPTION
WHEN OTHERS
THEN
     dbms_output.put_line( '******************************' );
     dbms_output.put_line( lStep );
     IF lStep = lProcName || '::' || 'Step 4::Reset Params after rebuild'
     THEN
         dbms_output.put_line('This step needs to be run');
     END IF;

     dbms_output.put_line( lSql );
     dbms_output.put_line (DBMS_UTILITY.format_error_stack ());
     dbms_output.put_line (DBMS_UTILITY.format_error_backtrace ());
     dbms_output.put_line( '******************************' );

END rebuildIndexPrc;


PROCEDURE moveTablePrc( pTableName         IN VARCHAR2
                       ,pMoveOption        IN VARCHAR2 DEFAULT 'PARALLEL(DEGREE 32) NOLOGGING'
                       ,pRebuildIndexes    IN VARCHAR2 DEFAULT 'Y'
                       ,pPostRebuildParams IN VARCHAR2 DEFAULT 'CURRENT' 
                       ,pTableSpace        IN VARCHAR2 DEFAULT NULL
                       ,pPartitionName     IN VARCHAR2 DEFAULT NULL
                       ,pSubPartitionName  IN VARCHAR2 DEFAULT NULL
                      )
IS
--
-- This procedure moves a table and rebuilds its indexes after moving.
-- Index statistics are gathered after rebuilding each index.
-- 
--   pTableName - name of the table to move
--   pMoveOption - options to use for moving the table. 
--      Default is 'PARALLEL(DEGREE 32) NOLOGGING'
--      NULL will use current settings
--   pRebuildIndexes - rebuild indexes after moving table.
--      Default is 'Y'
--   pPostRebuildParams - table parameters will be reset after move. 
--      Default is 'CURRENT', which are the current parameters prior to rebuilding.
--   pTableSpace - name of tablespace to move the table. 
--      Default value of NULL means current tablespace
--   pPartitionName - partition name if only moving a specific table partition
--   pSubPartitionName - subpartition name if only moving a specific table subpartition
-- 

  CURSOR move_cur 
  IS SELECT
            ut.table_name
           ,utp.partition_name
           ,utsp.subpartition_name
           --
           -- Alter table parameters prior to rebuilding
           -- Only the first row returned for each table will have a value
           --
           ,CASE WHEN ROW_NUMBER() OVER (PARTITION BY ut.table_name ORDER BY ut.table_name, utp.partition_name, utsp.subpartition_name ) = 1
                 THEN 'ALTER TABLE '|| ut.table_name || ' ' || pMoveOption
             END pMoveOption
           --
           -- Generate the sql to move the table, partitions and subpartitions if they exist
           --
           ,CASE WHEN utp.partition_name IS NULL
                 THEN 
                      'ALTER TABLE ' || ut.table_name || ' MOVE '|| ut.table_name
                 WHEN utsp.subpartition_name IS NULL 
                 THEN 
                      'ALTER TABLE ' || ut.table_name || ' MOVE PARTITION '|| utp.partition_name
                 ELSE 
                      'ALTER TABLE ' || ut.table_name || ' MOVE SUBPARTITION '|| utsp.subpartition_name
            END ||' '
            || CASE WHEN pTableSpace IS NULL THEN NULL ELSE ' TABLESPACE ' || pTableSpace || ' ' END AS move_stmt
           --  
           -- Reset table parameters after moving
           -- Only the last row returned for each table will have a value
           --
           ,CASE WHEN ROW_NUMBER() OVER (PARTITION BY ut.table_name ORDER BY ut.table_name DESC, utp.partition_name DESC, utsp.subpartition_name DESC ) = 1
                 THEN 
                 CASE WHEN pPostRebuildParams = 'CURRENT'
                      THEN  
                          'ALTER TABLE ' || ut.table_name || ' PARALLEL( DEGREE ' || ut.degree || ' INSTANCES '|| ut.instances ||') '|| 
                          CASE WHEN NVL( ut.logging, utp.logging )  = 'YES' THEN 'LOGGING' ELSE 'NOLOGGING' END
                      ELSE 
                          'ALTER TABLE ' || ut.table_name || ' ' || pPostRebuildParams
                  END
              END reset_stmt
            --
       FROM user_tables ut
       LEFT JOIN user_tab_partitions utp
         ON ut.table_name      = utp.table_name
       LEFT JOIN user_tab_subpartitions utsp
         ON utp.table_name      = utsp.table_name AND
            utp.partition_name  = utsp.partition_name
      WHERE ut.table_name       = pTableName AND
            ( pPartitionName    IS NULL OR
              pPartitionName    = utp.partition_name ) AND
            ( pSubPartitionName IS NULL OR
              pSubPartitionName = utsp.subpartition_name )  
      ORDER BY
           ut.table_name
          ,utp.partition_name
          ,utsp.subpartition_name;  

  
  lStep               VARCHAR2(100);
  lSql                VARCHAR2(2000);
  lDegree             VARCHAR2 (20);
  lInstances          VARCHAR2 (20);
  lCtr                PLS_INTEGER         := 0;
  lProcName           VARCHAR2  (30 BYTE) := 'moveTablePrc';
    
BEGIN

   --
   -- Alter session prior to starting process
   --
   EXECUTE IMMEDIATE 'alter session enable parallel dml';
   EXECUTE IMMEDIATE 'alter session set db_file_multiblock_read_count=128';
   EXECUTE IMMEDIATE 'alter session set ddl_lock_timeout=10';
   
   -- To reset degree and instances after move
   SELECT degree,instances
     INTO lDegree, lInstances
     FROM user_tables
    WHERE table_name = pTableName;


   FOR move_rec IN move_cur 
   LOOP
   
       lCtr := lCtr + 1;
       
       -- Alter table parameters prior to rebuilding
       -- This value is only populated for the first row returned for each table
       IF move_rec.pMoveOption IS NOT NULL 
       THEN
           lStep := lProcName || '::' || 'Step 1::Set pre-rebuild table options';
           lSql:=move_rec.pMoveOption;
           dbms_output.put_line( lStep || '::'|| lSql ||';' );
           EXECUTE IMMEDIATE lSql;
           dbms_output.put_line('********************');
           
       END IF;
       
       
       lStep := lProcName || '::' || 'Step 2:';
       lSql:=move_rec.move_stmt;
       dbms_output.put_line( lStep || '::' || lSql ||';' );
       EXECUTE IMMEDIATE lSql;
       
       
       -- This value is only populated for the last row for that index       
       IF move_rec.reset_stmt IS NOT NULL 
       THEN
           --
           -- Reset table parameters which were changed for the rebuild, generally to NOPARALLEL and LOGGING
           --
           dbms_output.put_line('********************');
           lStep := lProcName || '::' || 'Step 3::Reset Params after rebuild';
           lSql:=move_rec.reset_stmt;
           dbms_output.put_line( lStep || '::'|| lSql ||';' );
           EXECUTE IMMEDIATE lSql;
           dbms_output.put_line('********************' || CHR(10) );
       
       END IF;

   END LOOP;

   --
   -- call procedure to rebuild indexes
   -- Do not gather stats. 
   -- Stats will be gathered with table in the next step
   --
   lStep := lProcName || '::' || 'Step 4::Begin: Rebuild indexes after move';
   dbms_output.put_line('********************');
   dbms_output.put_line( lStep || '::' );
   IF pRebuildIndexes = 'Y'
   THEN
       rebuildIndexPrc( pTableName, pGatherStats=>'N' );
   
   END IF;
   dbms_output.put_line('********************');
   lStep := lProcName || '::' || 'Step 4::Complete: Rebuild indexes after move';
   dbms_output.put_line( lStep || '::' );
   dbms_output.put_line('********************' || CHR(10) );

   --
   -- Gather table statistics
   --
   lStep := lProcName || '::' || 'Step 5::Gather Table statistics';
   --dbms_output.put_line('********************');
   dbms_output.put_line( lStep || '::' );
   BEGIN
      SYS.DBMS_STATS.GATHER_TABLE_STATS (
         OwnName            => USER
        ,TabName            => pTableName
        ,Method_Opt         => 'FOR ALL COLUMNS SIZE AUTO '
        ,Degree             => 32
        ,Cascade            => TRUE
        ,Granularity        => 'AUTO'
        ,No_Invalidate      => FALSE
        ,force              => TRUE);
   END;
   dbms_output.put_line('********************' || CHR(10) );


EXCEPTION
WHEN OTHERS
THEN 
     dbms_output.put_line( '******************************' );
     dbms_output.put_line( lStep );
     dbms_output.put_line (DBMS_UTILITY.format_error_stack ());
     dbms_output.put_line (DBMS_UTILITY.format_error_backtrace ());
     dbms_output.put_line( '******************************' );
      
END moveTablePrc;

END db_util_pkg;
/
