set serveroutput on size 100000;
SPOOL doc_2prv.LOG

declare

b_OK Boolean := True;
B_OKRET   Boolean;
S_RET     varchar2(2000);
S_errmsg  varchar2(2000) := '';
S_STPMSG  varchar2(2000) := '';
S_TXTRET  varchar2(2000) := '';
N_CntOk   Integer Not Null := 0;
N_CntErr  Integer Not Null := 0;
N_CntBad  Integer Not Null := 0;
lv_IDBPDR Integer Not Null := 1;

Begin
   S_ERRMSG := '';
   ODBDOC_SRVC.SET_CHK_DTPRV('N');
   for tdoc in (select D.*
                       from odb$docday D
                       WHERE D.PrvDtOdb Is Null AND
                             D.IDFINTR = 1156605
               ) LOOP
       Begin
          IF tdoc.DayDtOdb is null then
             UPDATE odb$docday SET RDYIS = 'Y' WHERE IDDOC = TDOC.IDDOC AND RDYIS = 'N';
             UPDATE ODB$DOCDAY SET VRFIS = 'Y' WHERE IDDOC = TDOC.IDDOC AND VRFIS = 'N';
             S_STPMSG := 'CALL ODBDOC_SRVC.DAY_IDDOC';
             S_STPMSG := S_STPMSG || '('|| TO_CHAR(TDOC.IDDOC) ||', '||TO_CHAR(TDOC.DtOdb, 'DD/MM/YYYY')||')';
             S_RET := ODBDOC_SRVC.day_IDDOC(TDOC.IDDOC, TDOC.DtVAL, 'P');
             IF S_RET = 'Y' THEN
                S_ERRMSG := S_STPMSG || ' - OK';
                Commit;
                DBMS_OUTPUT.PUT_LINE(SUBSTR(S_ERRMSG,   1, 250));
                S_ERRMSG := '';
                N_CntOk := N_CntOk + 1;
                tdoc.DayDtOdb := TDOC.DtVAL;
             ELSIF S_RET = 'E' THEN
                Commit;
                DBMS_OUTPUT.PUT_LINE(SUBSTR(S_ERRMSG,   1, 250));
                DBMS_OUTPUT.PUT_LINE(SUBSTR(S_ERRMSG,  251,250));
                DBMS_OUTPUT.PUT_LINE(SUBSTR(S_ERRMSG,  501,250));
                DBMS_OUTPUT.PUT_LINE(SUBSTR(S_ERRMSG,  751,250));
                S_ERRMSG := '';
                N_CntBad := N_CntBad + 1;
             ELSE
                S_ERRMSG := S_STPMSG || NVL(S_TXTRET, ' - MESSAGE ERROR IS NULL, RETURN ['|| NVL(S_RET, 'NULL') ||']');
                DBMS_OUTPUT.PUT_LINE(SUBSTR(S_ERRMSG,   1, 250));
                DBMS_OUTPUT.PUT_LINE(SUBSTR(S_ERRMSG,  251,250));
                DBMS_OUTPUT.PUT_LINE(SUBSTR(S_ERRMSG,  501,250));
                DBMS_OUTPUT.PUT_LINE(SUBSTR(S_ERRMSG,  751,250));
                RollBack;
                N_CntErr := N_CntErr + 1;
             END IF;
             S_STPMSG := 'CALLED ODBCNTR_SRVC.NEXT_UNUMDOC';
          END IF;

          IF tdoc.DayDtOdb is not null then
             S_STPMSG := 'CALL ODBDOC_SRVC.PRV_IDDOC';
             S_STPMSG := S_STPMSG || '('|| TO_CHAR(TDOC.IDDOC) ||', '||TO_CHAR(TDOC.DayDtOdb, 'DD/MM/YYYY')||')';
             S_RET := ODBDOC_SRVC.PRV_IDDOC(TDOC.IDDOC, TDOC.DayDtOdb, 'P');
             IF S_RET = 'Y' THEN
                S_ERRMSG := S_STPMSG || ' - OK';
                Commit;
                DBMS_OUTPUT.PUT_LINE(SUBSTR(S_ERRMSG,   1, 250));
                S_ERRMSG := '';
                N_CntOk := N_CntOk + 1;
             ELSIF S_RET = 'E' THEN
                S_ERRMSG := S_STPMSG || ' - NOT';
                Commit;
                DBMS_OUTPUT.PUT_LINE(SUBSTR(S_ERRMSG,   1, 250));
                S_ERRMSG := '';
                N_CntBad := N_CntBad + 1;
             ELSE
                S_ERRMSG := S_STPMSG || NVL(S_TXTRET, ' - MESSAGE ERROR IS NULL, RETURN ['|| NVL(S_RET, 'NULL') ||']');
                DBMS_OUTPUT.PUT_LINE(SUBSTR(S_ERRMSG,   1, 250));
                DBMS_OUTPUT.PUT_LINE(SUBSTR(S_ERRMSG,  251,250));
                DBMS_OUTPUT.PUT_LINE(SUBSTR(S_ERRMSG,  501,250));
                DBMS_OUTPUT.PUT_LINE(SUBSTR(S_ERRMSG,  751,250));
                RollBack;
                N_CntErr := N_CntErr + 1;
             END IF;
          END IF;
          S_STPMSG := 'CALLED ODBCNTR_SRVC.NEXT_UNUMDOC';
       EXCEPTION
          WHEN OTHERS THEN
               S_ERRMSG := SUBSTR(S_STPMSG || CHR(10) || ' ÎØÈÁÊÀ ÒÅÑÒÎÂÎÉ ÏÐÎÖÅÄÓÐÛ:' || SQLERRM, 1, 1000);
               DBMS_OUTPUT.PUT_LINE(SUBSTR(S_ERRMSG,   1, 250));
               DBMS_OUTPUT.PUT_LINE(SUBSTR(S_ERRMSG,  251,250));
               DBMS_OUTPUT.PUT_LINE(SUBSTR(S_ERRMSG,  501,250));
               DBMS_OUTPUT.PUT_LINE(SUBSTR(S_ERRMSG,  751,250));
               RollBack;
               N_CntErr := N_CntErr + 1;
               S_ERRMSG := '';
       END;

       If N_CntErr > 0 Then
          Exit;
       ElsIf N_CntOk > 100 Then
          Exit;
       ElsIf N_CntBad > 10 Then
          Exit;
       End If;

   END LOOP;

   IF S_ERRMSG IS NULL THEN NULL;
   ELSE
      DBMS_OUTPUT.PUT_LINE(SUBSTR(S_ERRMSG,   1, 250));
      DBMS_OUTPUT.PUT_LINE(SUBSTR(S_ERRMSG,  251,250));
      DBMS_OUTPUT.PUT_LINE(SUBSTR(S_ERRMSG,  501,250));
      DBMS_OUTPUT.PUT_LINE(SUBSTR(S_ERRMSG,  751,250));
   END IF;
   DBMS_OUTPUT.PUT_LINE('CNT ERR:['|| TO_CHAR(N_CntErr) ||']');
   DBMS_OUTPUT.PUT_LINE('CNT BAD:['|| TO_CHAR(N_CntBad) ||']');
   DBMS_OUTPUT.PUT_LINE('CNT OK:['|| TO_CHAR(N_CntOk) ||']');
end;
/

SPOOL OFF;


