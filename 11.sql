CREATE OR REPLACE PACKAGE odb.ODB_$DOCPRV_SRVC AS

TYPE REC_CRDT_BOILER_ACNUM IS RECORD (ACNUM ODB.ODB$SYSACNT.ACNUM%TYPE);
TYPE TBL_CRDT_BOILER_ACNUM IS TABLE OF REC_CRDT_BOILER_ACNUM;

PROCEDURE CTX_CLEAR_CONTEXT(P_SESSID IN VARCHAR2,
                            P_PAR IN VARCHAR2 := NULL);

PROCEDURE CTX_SET_STOPING(P_SESSID IN VARCHAR2);
PROCEDURE CTX_SET_MOTHER(P_SESSID IN VARCHAR2);
PROCEDURE CTX_SET_MOTHER2(P_SESSID_MOTHER IN VARCHAR2);
PROCEDURE CTX_SET_SORT(P_SESSID IN VARCHAR2,
                         P_SORT IN INTEGER := 1);

--FUNCTION PF_DOCPRV(P_IDDOC IN ODB.ODB$DOCDAY.IDDOC%TYPE,
--                   P_DTODB DATE) RETURN INTEGER;

PROCEDURE PP_START_MANUAL(P_TYPE IN CHAR,
                          P_RETCODE OUT INTEGER,
                          P_MESSAGE OUT ODB.ODB$TMP_DOCPRVLOG.MESS%TYPE,
                          P_STEP IN INTEGER DEFAULT 0);

FUNCTION PF_START_RULE(P_IDRULE IN ODB$PRVRULES.IDRULE%TYPE,
                        P_RETCODE OUT INTEGER,
                        P_MESSAGE OUT ODB.ODB$TMP_DOCPRVLOG.MESS%TYPE)
                        RETURN INTEGER;

PROCEDURE PP_SESSION_INIT(P_CONNECT_DISCONNECT INTEGER,
                          P_RETCODE OUT INTEGER,
                          P_ISAUTO IN ODB$PRVSESSIONS.IS_AUTO%TYPE DEFAULT 'Y');

PROCEDURE PP_DOCS2PACK(P_PACK IN VARCHAR2);

TYPE TYPE_IDDOC_REC IS RECORD (IDDOC ODB$DOCDAY.IDDOC%TYPE);
TYPE TYPE_DOCDAY_TBL IS TABLE OF TYPE_IDDOC_REC;
FUNCTION PF_DOCTRAIN(P_IDDOC IN ODB$DOCDAY.IDDOC%TYPE)
   RETURN TYPE_DOCDAY_TBL PIPELINED;

PROCEDURE PP_DOCS2PRNLOG;

--Процедура браковки пакета документов
PROCEDURE PP_$DOCSBRAK(P_REASON IN VARCHAR2,
                       P_ENDTRAN BOOLEAN);

--Процедура браковки пакета документов в авт. транзакции
PROCEDURE PP_DOCSBRAK(P_REASON IN VARCHAR2);

PROCEDURE PP_NDOCS_PERMIT;

FUNCTION PF_GET_CRDTBOILER(P_DTODB IN ODB$DOCDAY.DTVAL%TYPE) RETURN TBL_CRDT_BOILER_ACNUM PIPELINED;

PROCEDURE PP_DOCSPUT(P_PUT IN INTEGER);

PROCEDURE PP_DOCS2NEXTDAY(P_DT IN DATE);

PRAGMA RESTRICT_REFERENCES(PF_DOCTRAIN, WNDS, WNPS);
PRAGMA RESTRICT_REFERENCES(PF_GET_CRDTBOILER, WNDS, WNPS);

END ODB_$DOCPRV_SRVC;
/
CREATE OR REPLACE PACKAGE BODY odb.ODB_$DOCPRV_SRVC AS

   LPB_EXCEPT      EXCEPTION;

--Описание контекста ODB_CTX_PRVAUTO (для управления несколькими сессиями)
--      TREAD_STOPING    - ФЛАГ ОСТАНОВКИ УКАЗАННОЙ СЕССИИ
--      TREAD_MOTHER     - ИДЕНТИФИКАТОР МАТЕРИНСКОЙ СЕССИИ
--      TREAD_SORT       - ФЛАГ СОРТИРОВКИ (ASC / DESC)
--      TREAD_RESULT     - ХОД ВЫПОЛНЕНИЯ ДЛЯ РУЧНОЙ ПРОВОДКИ
--
--  Описание контекста ODB_CTX_PRVLOCAL (для локального использования)
--      LOCAL_DTODB      - ТЕКУЩАЯ ДАТА ОДБ(DD.MM.YYYY)

--ОЧИСТКА ПАРАМЕТРА КОНТЕКСТА.
--  ЕСЛИ P_PAR IS NULL, ТО ОЧИЩАЮТСЯ ВСЕ ПАРАМЕТРЫ ДЛЯ P_SESSID

PROCEDURE CTX_CLEAR_CONTEXT(P_SESSID IN VARCHAR2,
                            P_PAR IN VARCHAR2 := NULL) AS

BEGIN
   DBMS_SESSION.CLEAR_CONTEXT('ODB_CTX_PRVAUTO', P_SESSID, P_PAR);
END CTX_CLEAR_CONTEXT;

--ОСТАНОВКА ПРОЦЕССА
PROCEDURE CTX_SET_STOPING(P_SESSID IN VARCHAR2) AS
BEGIN
   DBMS_SESSION.SET_CONTEXT('ODB_CTX_PRVAUTO', 'TREAD_STOPING', '1', USER, P_SESSID);
END CTX_SET_STOPING;

--УСТАНАВЛИВАЕМ ИДЕНТИФИКАТОР МАТЕРИНСКОЙ СЕССИИ ДЛЯ УКАЗАННОЙ
PROCEDURE CTX_SET_MOTHER(P_SESSID IN VARCHAR2) AS
BEGIN
   DBMS_SESSION.SET_CONTEXT('ODB_CTX_PRVAUTO', 'TREAD_MOTHER', DBMS_SESSION.UNIQUE_SESSION_ID, USER, P_SESSID);
END CTX_SET_MOTHER;

--УСТАНАВЛИВАЕМ ИДЕНТИФИКАТОР МАТЕРИНСКОЙ СЕССИИ ДЛЯ ТЕКУЩЕЙ
PROCEDURE CTX_SET_MOTHER2(P_SESSID_MOTHER IN VARCHAR2) AS
BEGIN
   DBMS_SESSION.SET_CONTEXT('ODB_CTX_PRVAUTO', 'TREAD_MOTHER', P_SESSID_MOTHER, USER, DBMS_SESSION.UNIQUE_SESSION_ID);
END CTX_SET_MOTHER2;

--УСТАНАВЛИВАЕМ СОРТИРОВКУ ОБРАБОТКИ ДОКУМЕНТОВ
--P_SORT =
--      1 - ПРЯМАЯ СОРТИРОВКА (ПО ДАТЕ И ВРЕМЕНИ ПОСТУПЛЕНИЯ ДОКУМЕНТОВ НА ПРОВОДКУ)
--      0 - ОБРАТНАЯ

PROCEDURE CTX_SET_SORT(P_SESSID IN VARCHAR2,
                         P_SORT IN INTEGER := 1) AS
   LP_SORT       VARCHAR2(4) := 'ASC';
BEGIN
   IF P_SORT = 0 THEN
      LP_SORT := 'DESC';
   END IF;
   DBMS_SESSION.SET_CONTEXT('ODB_CTX_PRVAUTO', 'TREAD_SORT', LP_SORT, USER, P_SESSID);

END CTX_SET_SORT;

--ФУНКЦИЯ ЗАПУСКА РУЧНОЙ ПРОВОДКИ/РЕПРОВОДКИ ВЫБРАННЫХ ДОКУМЕНТОВ
PROCEDURE PP_START_MANUAL(P_TYPE IN CHAR,
                         P_RETCODE OUT INTEGER,
                         P_MESSAGE OUT ODB.ODB$TMP_DOCPRVLOG.MESS%TYPE,
                         P_STEP IN INTEGER DEFAULT 0) AS
PRAGMA AUTONOMOUS_TRANSACTION;
--P_TYPE = 'P'-ПРОВЕСТИ, 'R'-РЕПРОВЕСТИ
--  P_RETCODE = КОЛИЧЕСТВО ПРОВЕДЕННЫХ ДОКУМЕНТОВ
--            -99  - ОШИБКА SQL ЛИБО ПРОЦЕДУРЫ

 LP_MESS         VARCHAR2(2000);
 LP_RET          VARCHAR2(1);
 LP_ERDOC        ODB$DOCDAY.ERDOC%TYPE;
 LP_ERDMFO       ODB$DOCDAY.ERDMFO%TYPE;
 LP_ERDACNUM     ODB$DOCDAY.ERDACNUM%TYPE;
 LP_ERKMFO       ODB$DOCDAY.ERKMFO%TYPE;
 LP_ERKACNUM     ODB$DOCDAY.ERKACNUM%TYPE;

 LP_CNT_READY    INTEGER := 0;  --КОЛ-ВО ОБРАБОТАННЫХ ДОКУМЕНТОВ
 LP_CNT_SKIP     INTEGER := 0;  --КОЛ-ВО ПРОПУЩЕННЫХ ДОКУМЕНТОВ
 LP_CNT_ALL      INTEGER := 0;  --КОЛ-ВО ПРОСМОТРЕННЫХ ДОКУМЕНТОВ
 LP_CNT_ERR      INTEGER := 0;  --КОЛ-ВО ДОКУМЕНТОВ С ОШИБКАМИ
 LP_ITERATION    INTEGER := 10000;
 LP_COUNT_ROWS   INTEGER;
 LP_RESULT       VARCHAR2(1000);
 LP_DTODB        DATE;

 TREC_ODBDAY      ODBCNFDAY%ROWTYPE;
 TREC_ODBDAYSTATE ODBCNFDAYSTATE%ROWTYPE;
BEGIN
   IF P_STEP = 0 THEN
      IF P_TYPE IS NULL THEN
         LP_MESS := 'НЕ УКАЗАН ПРИЗНАК "ПРОВОДКА/РЕПРОВОДКА"';
         RAISE LPB_EXCEPT;
      ELSIF P_TYPE NOT IN ('P', 'R') THEN
         LP_MESS := 'НЕВЕРНО УКАЗАН ПРИЗНАК "ПРОВОДКА/РЕПРОВОДКА"';
         RAISE LPB_EXCEPT;
      END IF;
   END IF;

   LP_MESS := 'ОПРЕДЕЛЯЕМ ТЕКУЩИЙ ОПЕРАЦИОННЫЙ ДЕНЬ БАНКА. ';
   SELECT * INTO TREC_ODBDAY FROM ODBCNFDAY;

   IF TREC_ODBDAY.ODB_STATE = 'C' THEN
      LP_MESS := LP_MESS || 'ДЕНЬ ' || TO_CHAR(TREC_ODBDAY.ODB_DATE, 'DD.MM.YYYY') || ' УЖЕ ЗАКРЫТ, А НОВЫЙ ЕЩЕ НЕ ОТКРЫТ.';
      RAISE LPB_EXCEPT;
   END IF;

   LP_DTODB := TO_DATE(SYS_CONTEXT('ODB_CTX_PRVLOCAL', 'LOCAL_DTODB'), 'DD.MM.YYYY');
   IF P_STEP = 0 THEN
      LP_MESS := 'ПРОВЕРЯЕМ СОСТОЯНИЕ ПРОЦЕССА ЗАКРЫТИЯ ДНЯ';
      SELECT * INTO TREC_ODBDAYSTATE FROM ODBCNFDAYSTATE;
      IF TREC_ODBDAYSTATE.FOX_CLS = 'Y' THEN
         LP_MESS := 'ВЫПОЛНЯЕТСЯ ОПЕРАЦИЯ ЗАКРЫТИЯ ОПЕРАЦИОННОГО ДНЯ. ПРОВОДКА ЗАПРЕЩЕНА!!!';
         RAISE LPB_EXCEPT;
      END IF;

      IF NVL(LP_DTODB, TREC_ODBDAY.ODB_DATE) <> TREC_ODBDAY.ODB_DATE THEN
         LP_MESS := 'ИЗМЕНИЛСЯ ТЕКУЩИЙ ОПЕРАЦИОННЫЙ ДЕНЬ!!! НЕОБХОДИМО ЗАЙТИ В СИСТЕМУ ЗАНОВО';
         RAISE LPB_EXCEPT;
      END IF;
      DBMS_SESSION.SET_CONTEXT('ODB_CTX_PRVLOCAL', 'LOCAL_DTODB', TO_CHAR(TREC_ODBDAY.ODB_DATE, 'DD.MM.YYYY'));
   END IF;

   LP_DTODB := TREC_ODBDAY.ODB_DATE;
   P_RETCODE := 0;

   SELECT COUNT(*) INTO LP_COUNT_ROWS
      FROM ODB$TMP_DOC4PRV T
      WHERE (P_STEP = 0 OR (P_STEP > 0 AND T.MESS = 'E'));

   IF P_TYPE = 'P' THEN
      APP_ADDLOG('ПРОВОДКА ДОКУМЕНТОВ: СТАРТ (ШАГ '||TO_CHAR(P_STEP)||'. ВЫБРАНО: ' || TO_CHAR(LP_COUNT_ROWS) || ' ДОК.)', 'MPRVDOC');
   ELSE
      APP_ADDLOG('РЕПРОВОДКА ДОКУМЕНТОВ: СТАРТ (ШАГ '||TO_CHAR(P_STEP)||'. ВЫБРАНО: ' || TO_CHAR(LP_COUNT_ROWS) || ' ДОК.)', 'MPRVDOC');
   END IF;

   ODBDOC_SRVC.SET_SYSACNUMS4PRV(LP_DTODB);
   ODBDOC_SRVC.SET_ACRISIS;
   ODBDOC_SRVC.SET_REGLAMENT;

   FOR TROWS IN 1 .. CEIL(LP_COUNT_ROWS / LP_ITERATION)
   LOOP
      FOR TREC IN (SELECT IDDOC FROM (SELECT T.IDDOC, ROWNUM AS R_NUM
                                         FROM ODB$TMP_DOC4PRV T
                                         WHERE ROWNUM < TROWS * LP_ITERATION + 1 AND
                                               (P_STEP = 0 OR (P_STEP > 0 AND T.MESS = 'E'))
                                      )
                      WHERE R_NUM > (TROWS - 1) * LP_ITERATION)
      LOOP
         EXIT WHEN NVL(TO_NUMBER(SYS_CONTEXT('ODB_CTX_PRVAUTO', 'TREAD_STOPING')), 0) = 1;
         SAVEPOINT PRV_IDDOC;

         DECLARE
            LL_IDDOC     ODB$DOCDAY.IDDOC%TYPE;
         BEGIN
            LP_MESS := 'БЛОКИРУЕМ ДОКУМЕНТ ID='||TO_CHAR(TREC.IDDOC)||' ДЛЯ ПРОВЕДЕНИЯ';
            LP_RET := 'Y';
            SELECT IDDOC INTO LL_IDDOC
               FROM ODB$DOCDAY
               WHERE IDDOC = TREC.IDDOC
               FOR UPDATE NOWAIT;
         EXCEPTION
            WHEN NO_DATA_FOUND THEN
               LP_MESS := LP_MESS || ' - ЗАПИСЬ НЕ НАЙДЕНА';
               LP_RET := 'N';
            WHEN OTHERS THEN
               IF SQLCODE = -54 THEN
                  LP_MESS := LP_MESS || ' - БЛОКИРОВАН ДРУГИМ ПОЛЬЗОВАТЕЛЕМ!!!';
               ELSE
                  LP_MESS := LP_MESS || CHR(10) || SQLERRM;
               END IF;
               LP_RET := 'N';
         END;

         IF LP_RET = 'Y' THEN
            IF P_TYPE = 'P' THEN
               --ПРОВОДКА
               BEGIN
                  LP_MESS := 'ДОКУМЕНТ ID='||TO_CHAR(TREC.IDDOC)||' ПЕРЕВОДИМ В ДОКУМЕНТЫ ДНЯ';
                  LP_RET := ODBDOC_SRVC.DAY_IDDOC(TREC.IDDOC, LP_DTODB, P_TYPE);
               EXCEPTION
                  WHEN OTHERS THEN
                     LP_MESS := LP_MESS || CHR(10) || SQLERRM;
                     LP_RET := 'N';
               END;

               IF LP_RET = 'Y' THEN
                  BEGIN
                     LP_MESS := 'ВЫПОЛНЯЕМ ПРОВОДКУ ДОКУМЕНТА ID='||TO_CHAR(TREC.IDDOC);
                     LP_RET := ODBDOC_SRVC.PRV_IDDOC(TREC.IDDOC, LP_DTODB, P_TYPE);
                  EXCEPTION
                     WHEN OTHERS THEN
                        LP_MESS := LP_MESS || CHR(10) || SQLERRM;
                        LP_RET := 'N';
                  END;

               END IF;
            ELSE
               --РЕПРОВОДКА
               BEGIN
                  LP_MESS := 'ВЫПОЛНЯЕМ РЕПРОВОДКУ ДОКУМЕНТА ID='||TO_CHAR(TREC.IDDOC);
                  LP_RET := ODBDOC_SRVC.PRV_IDDOC(TREC.IDDOC, LP_DTODB, P_TYPE);
               EXCEPTION
                  WHEN OTHERS THEN
                     LP_MESS := LP_MESS || CHR(10) || SQLERRM;
                     LP_RET := 'N';
               END;

               IF LP_RET = 'Y' THEN
                  BEGIN
                     LP_MESS := 'ДОКУМЕНТ ID='||TO_CHAR(TREC.IDDOC)||' ВОЗВРАЩАЕМ ИЗ ДОКУМЕНТОВ ДНЯ';
                     LP_RET := ODBDOC_SRVC.DAY_IDDOC(TREC.IDDOC, LP_DTODB, P_TYPE);
                  EXCEPTION
                     WHEN OTHERS THEN
                        LP_MESS := LP_MESS || CHR(10) || SQLERRM;
                        LP_RET := 'N';
                  END;
               END IF;
            END IF;
         END IF;

         --ПРОВЕРКА ОБРАБОТКИ
         IF LP_RET = 'Y' THEN
            LP_CNT_READY := LP_CNT_READY + 1;
            UPDATE ODB$TMP_DOC4PRV D SET D.MESS = NULL
               WHERE IDDOC = TREC.IDDOC;
            DELETE FROM ODB$DOCDAY_PRVPUT WHERE IDDOC = TREC.IDDOC;
         ELSIF LP_RET = 'E' THEN
            LP_CNT_ERR := LP_CNT_ERR + 1;

            LP_MESS := 'ЧИТАЕМ ОШИБКИ НА ДОКУМЕНТЕ ID='||TO_CHAR(TREC.IDDOC)|| ' ДЛЯ ОТМЕНЫ ТРАНЗАКЦИИ';
            SELECT ERDOC, ERDMFO, ERDACNUM, ERKMFO, ERKACNUM
               INTO LP_ERDOC, LP_ERDMFO, LP_ERDACNUM, LP_ERKMFO, LP_ERKACNUM
               FROM ODB$DOCDAY
               WHERE IDDOC = TREC.IDDOC;

            ROLLBACK TO PRV_IDDOC;

            LP_MESS := 'ЗАПИСЫВАЕМ ОШИБКИ ДОКУМЕНТА ID='||TO_CHAR(TREC.IDDOC)||' ДЛЯ ОТОБРАЖЕНИЯ';
            UPDATE ODB$TMP_DOC4PRV D SET D.MESS = LP_RET
               WHERE IDDOC = TREC.IDDOC;

            LP_MESS := 'ЗАПИСЫВАЕМ ОШИБКИ НА ДОКУМЕНТЕ ID='||TO_CHAR(TREC.IDDOC);
            UPDATE ODB$DOCDAY
               SET ERDOC    = LP_ERDOC,
                   ERDMFO   = LP_ERDMFO,
                   ERDACNUM = LP_ERDACNUM,
                   ERKMFO   = LP_ERKMFO,
                   ERKACNUM = LP_ERKACNUM
               WHERE IDDOC  = TREC.IDDOC;
         ELSE /*N*/
            LP_CNT_SKIP := LP_CNT_SKIP + 1;
            ROLLBACK TO PRV_IDDOC;
            UPDATE ODB$TMP_DOC4PRV D
               SET D.MESS = NVL(LP_MESS, 'НЕОПРЕДЕЛЁННАЯ ОШИБКА')
               WHERE IDDOC = TREC.IDDOC;
         END IF;

         COMMIT;

         LP_CNT_ALL := LP_CNT_ALL + 1;

         LP_RESULT := TO_CHAR(LP_CNT_ALL) || '|' ||
                      TO_CHAR(LP_CNT_READY) || '|' ||
                      TO_CHAR(LP_CNT_SKIP) || '|' ||
                      TO_CHAR(LP_CNT_ERR) || '|' || TO_CHAR(P_STEP) || '|';
         DBMS_SESSION.SET_CONTEXT('ODB_CTX_PRVAUTO', 'TREAD_RESULT', LP_RESULT, USER, SYS_CONTEXT('ODB_CTX_PRVAUTO', 'TREAD_MOTHER'));
      END LOOP;
      EXIT WHEN NVL(TO_NUMBER(SYS_CONTEXT('ODB_CTX_PRVAUTO', 'TREAD_STOPING')), 0) = 1;
   END LOOP;

   IF NVL(TO_NUMBER(SYS_CONTEXT('ODB_CTX_PRVAUTO', 'TREAD_STOPING')), 0) = 1 THEN
      NULL;
   ELSE
      --ВЫПОЛНЯЕМ СЛЕДУЮЩИЙ ЦИКЛ ПРОВОДКИ ДЛЯ ДОКУМЕНТОВ С ОШИБКАМИ"
      IF LP_CNT_ERR > 0 AND LP_CNT_READY > 0 THEN
         ODB_$DOCPRV_SRVC.PP_START_MANUAL(P_TYPE, P_RETCODE, LP_MESS, P_STEP + 1);
      END IF;
   END IF;
   LP_CNT_READY := LP_CNT_READY + P_RETCODE;

   IF P_STEP = 0 THEN
      DELETE FROM ODB$TMP_DOC4PRV D WHERE MESS IS NULL;
      --СЧИТАЕМ РЕЗУЛЬТАТЫ
      SELECT (SELECT COUNT(*) FROM ODB$TMP_DOC4PRV WHERE LENGTH(MESS) > 2) AS SKIP,
             (SELECT COUNT(*) FROM ODB$TMP_DOC4PRV WHERE LENGTH(MESS) = 2 OR MESS = 'E') AS ERR
         INTO LP_CNT_SKIP,
              LP_CNT_ERR
         FROM DUAL;

      P_MESSAGE := 'Просмотрено - '||TO_CHAR(LP_CNT_ALL) || CHR(10) ||
                   'из них: обработано - '||TO_CHAR(LP_CNT_READY) || CHR(10) ||
                   '        пропущено - '||TO_CHAR(LP_CNT_SKIP) || CHR(10) ||
                   '        ошибочных - '||TO_CHAR(LP_CNT_ERR);
   END IF;

   IF NVL(TO_NUMBER(SYS_CONTEXT('ODB_CTX_PRVAUTO', 'TREAD_STOPING')), 0) = 1 THEN
      P_MESSAGE := P_MESSAGE || CHR(10)||' ОБРАБОТКА ПРЕРВАНА';
   END IF;

   LP_MESS := 'ОЧИЩАЕМ ВРЕМЕННЫЕ ДАННЫЕ';
   DELETE FROM ODB$TMP_DOC4PRV D WHERE MESS IS NULL;

   IF P_STEP = 0 THEN
      IF P_TYPE = 'P' THEN
         APP_ADDLOG('ПРОВОДКА ДОКУМЕНТОВ: ФИНИШ '|| CHR(10) || P_MESSAGE||')', 'MPRVDOC');
      ELSE
         APP_ADDLOG('РЕПРОВОДКА ДОКУМЕНТОВ: ФИНИШ '|| CHR(10) || P_MESSAGE||')', 'MPRVDOC');
      END IF;
      PP_SESSION_INIT(0, P_RETCODE);
   END IF;

   P_RETCODE := LP_CNT_READY;

   COMMIT;
EXCEPTION
   WHEN LPB_EXCEPT THEN
      P_RETCODE := -99;
      P_MESSAGE := 'ПРОВОДКА ВЫБРАННЫХ ДОКУМЕНТОВ. ' || LP_MESS || CHR(10)|| TO_CHAR(SQLCODE) || ' - ' || SQLERRM;
      APP_ADDLOG(P_MESSAGE, 'MPRVDOC');
      ROLLBACK;
   WHEN OTHERS THEN
      P_RETCODE := -99;
      P_MESSAGE := 'ПРОВОДКА ВЫБРАННЫХ ДОКУМЕНТОВ. ' || LP_MESS || CHR(10)|| TO_CHAR(SQLCODE) || ' - ' || SQLERRM;
      APP_ADDLOG(P_MESSAGE, 'MPRVDOC');
      ROLLBACK;
END PP_START_MANUAL;

--ФУНКЦИЯ ЗАПУСКА АВТОМАТИЧЕСКОЙ ПРОВОДКИ
FUNCTION PF_START_RULE(P_IDRULE IN ODB$PRVRULES.IDRULE%TYPE,
                        P_RETCODE OUT INTEGER,
                        P_MESSAGE OUT ODB.ODB$TMP_DOCPRVLOG.MESS%TYPE)
                        RETURN INTEGER AS
--RETURN = КОЛИЧЕСТВО ПРОВЕДЕННЫХ ДОКУМЕНТОВ
--  P_RETCODE:
--     0  - НОРМАЛЬНОЕ ЗАВЕРШЕНИЕ (ЗАПОЛНЯЕТСЯ P_MESSAGE)
--    -1  - ПРЕРВАНО
--    -2  - ПРАВИЛО НЕ НАЙДЕНО
--    -3  - ПРАВИЛО НЕ АКТИВНО
--    -4  - НЕТ УСЛОВИЯ ПО ПРАВИЛУ
--    -5  - РАЗРЫВ СОЕДИНЕНИЯ ОСНОВНОЙ СЕССИИ
--    -6  - СЛИШКОМ МНОГО УСЛОВИЙ
--    -7  - СЕССИЯ ПРОВОДКИ НЕ ЗАРЕГИСТРИРОВАНА
--    -8  - ВЫПОЛНЯЕТСЯ ЗАКРЫТИЕ ДНЯ
--   -99  - ОШИБКА SQL ЛИБО ПРОЦЕДУРЫ

 LP_MESS         VARCHAR2(2000);
 LP_RET          VARCHAR2(1);
 LP_CNT_READY    INTEGER := 0;  --КОЛ-ВО ОБРАБОТАННЫХ ДОКУМЕНТОВ
 LP_CNT_SKIP     INTEGER := 0;  --КОЛ-ВО ПРОПУЩЕННЫХ ДОКУМЕНТОВ
 LP_CNT_ERR      INTEGER := 0;

 LP_ERDOC        ODB$DOCDAY.ERDOC%TYPE;
 LP_ERDMFO       ODB$DOCDAY.ERDMFO%TYPE;
 LP_ERDACNUM     ODB$DOCDAY.ERDACNUM%TYPE;
 LP_ERKMFO       ODB$DOCDAY.ERKMFO%TYPE;
 LP_ERKACNUM     ODB$DOCDAY.ERKACNUM%TYPE;

 LP_ISACTIVE     ODB.ODB$PRVRULES.ISACTIVE%TYPE;
 LP_CRITERION    VARCHAR2(32000);
 LP_SQL          VARCHAR2(32767);
 LP_CLIDENTIFIER VARCHAR2(64);
 LP_TREAD_MOTHER VARCHAR2(64);
 LP_DTODB        DATE;

 LP_BEGIN        DATE;
 LP_TIMEOUT      INTEGER;

 TYPE T_DOCS4PRV IS REF CURSOR;
 CUR_DOCS4PRV    T_DOCS4PRV;
 LP_IDDOC        ODB.ODB$DOCDAY.IDDOC%TYPE;
-- LP_DTODB        DATE;
 LP_SORT         VARCHAR2(40);

 TREC_ODBDAY      ODBCNFDAY%ROWTYPE;
 TREC_ODBDAYSTATE ODBCNFDAYSTATE%ROWTYPE;
 LP_NUM_WORK      ODB$DOCPRVLOG.NUM_WORK%TYPE := 0;
BEGIN
   LP_CLIDENTIFIER := SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER');
   LP_TREAD_MOTHER := SYS_CONTEXT('ODB_CTX_PRVAUTO', 'TREAD_MOTHER');
   CTX_CLEAR_CONTEXT(LP_CLIDENTIFIER, 'TREAD_STOPING');

   DECLARE
      LI_CNT    INTEGER;
   BEGIN
      LP_MESS := 'ПРОВЕРЯЕМ РЕГИСТРАЦИЮ СЕССИИ';
      SELECT 1 INTO LI_CNT FROM DUAL
         WHERE EXISTS (SELECT 1 FROM ODB$PRVSESSIONS S WHERE S.IDSESSION = LP_CLIDENTIFIER);
   EXCEPTION
      WHEN NO_DATA_FOUND THEN
         P_RETCODE := -7;
         P_MESSAGE := 'СЕССИЯ АВТОМАТИЧЕСКОЙ ПРОВОДКИ НЕ ЗАРЕГИСТРИРОВАНА';
         RAISE LPB_EXCEPT;
   END;

   P_RETCODE := 0;

   LP_MESS := 'ОПРЕДЕЛЯЕМ ТЕКУЩИЙ ОПЕРАЦИОННЫЙ ДЕНЬ БАНКА. ';
   SELECT * INTO TREC_ODBDAY FROM ODBCNFDAY;

   IF TREC_ODBDAY.ODB_STATE = 'C' THEN
      LP_MESS := LP_MESS || 'ДЕНЬ ' || TO_CHAR(TREC_ODBDAY.ODB_DATE, 'DD.MM.YYYY') || ' УЖЕ ЗАКРЫТ, А НОВЫЙ ЕЩЕ НЕ ОТКРЫТ.';
      RAISE LPB_EXCEPT;
   END IF;

   LP_MESS := 'ПРОВЕРЯЕМ СОСТОЯНИЕ ПРОЦЕССА ЗАКРЫТИЯ ДНЯ';
   SELECT * INTO TREC_ODBDAYSTATE FROM ODBCNFDAYSTATE;
   IF TREC_ODBDAYSTATE.FOX_CLS = 'Y' THEN
      P_RETCODE := -8;
      P_MESSAGE := 'ВЫПОЛНЯЕТСЯ ОПЕРАЦИЯ ЗАКРЫТИЯ ОПЕРАЦИОННОГО ДНЯ. ПРОВОДКА ЗАПРЕЩЕНА!!!';
      RAISE LPB_EXCEPT;
   END IF;

   LP_MESS := 'ОПРЕДЕЛЯЕМ ПРАВИЛО IDRULE=' || TO_CHAR(P_IDRULE);
   BEGIN
      SELECT DECODE(PR.ISACTIVE, PR.ISVRF, 'Y', 'N')
         INTO LP_ISACTIVE
         FROM ODB.ODB$PRVRULES PR
         WHERE PR.IDRULE = P_IDRULE;

      IF LP_ISACTIVE = 'N' THEN
         P_RETCODE := -3;
         P_MESSAGE := 'ПРАВИЛО НЕ АКТИВНО';
         RAISE LPB_EXCEPT;
      END IF;

      LP_MESS := 'СОСТАВЛЯЕМ СТРОКУ УСЛОВИЙ ПРАВИЛА IDRULE=' || TO_CHAR(P_IDRULE);
      FOR TREC_CRIT IN (SELECT NVL2(P.DMFO_S,   ' AND D.DMFO '   || ODB_$DOCPRVADM_SRVC.TBL2STRING_CHAR(P.DMFO,   ',', P.DMFO_S),   '') ||
                               NVL2(P.DACNUM_S, ' AND D.DACNUM ' || ODB_$DOCPRVADM_SRVC.TBL2STRING_CHAR(P.DACNUM, ',', P.DACNUM_S), '') ||
                               NVL2(P.KMFO_S,   ' AND D.KMFO '   || ODB_$DOCPRVADM_SRVC.TBL2STRING_CHAR(P.KMFO,   ',', P.KMFO_S),   '') ||
                               NVL2(P.KACNUM_S, ' AND D.KACNUM ' || ODB_$DOCPRVADM_SRVC.TBL2STRING_CHAR(P.KACNUM, ',', P.KACNUM_S), '') ||
                               NVL2(P.KPCKG_S,  ' AND D.KPCKG '  || ODB_$DOCPRVADM_SRVC.TBL2STRING_CHAR(P.KPCKG,  ',', P.KPCKG_S),  '') ||
                               NVL2(P.IDISP_S,  ' AND D.IUSRNM ' || ODB_$DOCPRVADM_SRVC.TBL2STRING_CHAR(P.IDISP,  ',', P.IDISP_S),  '') ||
                               NVL2(P.IDSYS_S,  ' AND D.IDSYS '  || ODB_$DOCPRVADM_SRVC.TBL2STRING_CHAR(P.IDSYS,  ',', P.IDSYS_S),  '') ||
                               NVL2(P.IDPDR_S,  ' AND D.IDPDR '  || ODB_$DOCPRVADM_SRVC.TBL2STRING_INT (P.IDPDR,  ',', P.IDPDR_S),  '') ||
                               NVL2(P.KDOC_S,   ' AND D.KDOC '   || ODB_$DOCPRVADM_SRVC.TBL2STRING_CHAR(P.KDOC,   ',', P.KDOC_S),   '') ||
                               NVL2(P.SGNDOC_S, ' AND D.SGNDOC ' || ODB_$DOCPRVADM_SRVC.TBL2STRING_CHAR(P.SGNDOC,   ',', P.SGNDOC_S), '') ||
                               NVL2(P.KV_S,     ' AND D.KV '     || ODB_$DOCPRVADM_SRVC.TBL2STRING_CHAR(P.KV,     ',', P.KV_S),     '') AS val
                          FROM ODB$PRVCRIT P
                         WHERE P.IDRULE = P_IDRULE AND
                               DECODE(P.ISACTIVE, P.ISVRF, 'Y', 'N') = 'Y'
                        )
      LOOP
         IF TREC_CRIT.VAL IS NOT NULL THEN
            IF LENGTH(LP_CRITERION) > 0 THEN
               LP_CRITERION := LP_CRITERION || ' OR ';
            END IF;
            LP_CRITERION :=  LP_CRITERION || '(' || SUBSTR(TREC_CRIT.VAL, 6, LENGTH(TREC_CRIT.VAL) - 5) || ')';
         END IF;
      END LOOP;

      IF LP_CRITERION IS NULL THEN
         P_RETCODE := -4;
         P_MESSAGE := 'НЕТ АКТИВНЫХ УСЛОВИЙ ОТБОРА ДОКУМЕНТОВ';
         RAISE LPB_EXCEPT;
      ELSE
         LP_CRITERION := ' AND (' || LP_CRITERION || ')';
      END IF;
   EXCEPTION
      WHEN VALUE_ERROR THEN
         P_RETCODE := -6;
         P_MESSAGE := 'СЛИШКОМ МНОГО УСЛОВИЙ ДЛЯ ПРАВИЛА';
         RAISE LPB_EXCEPT;
      WHEN NO_DATA_FOUND THEN
         P_RETCODE := -2;
         P_MESSAGE := 'ПРАВИЛО ОТСУТСТВУЕТ';
         RAISE LPB_EXCEPT;
   END;

   LP_CRITERION := REPLACE(LP_CRITERION, '?', '_');
   LP_CRITERION := REPLACE(LP_CRITERION, '*', '%');

   LOOP
      ODBDOC_SRVC.SET_SYSACNUMS4PRV(TREC_ODBDAY.ODB_DATE);
      ODBDOC_SRVC.SET_ACRISIS;
      ODBDOC_SRVC.SET_REGLAMENT;
      LP_CNT_READY := 0;
      LP_CNT_SKIP := 0;
      LP_CNT_ERR := 0;
      IF NVL(TO_NUMBER(SYS_CONTEXT('ODB_CTX_PRVAUTO', 'TREAD_STOPING')), 0) = 1 THEN
         P_RETCODE := -1;
         EXIT;
      END IF;
      LP_NUM_WORK := LP_NUM_WORK + 1;

      LP_DTODB := TO_DATE(SYS_CONTEXT('ODB_CTX_PRVLOCAL', 'LOCAL_DTODB'), 'DD.MM.YYYY');

      IF NVL(LP_DTODB, TREC_ODBDAY.ODB_DATE) <> TREC_ODBDAY.ODB_DATE THEN
         P_RETCODE := -8;
         P_MESSAGE := 'ИЗМЕНИЛСЯ ТЕКУЩИЙ ОПЕРАЦИОННЫЙ ДЕНЬ!!! НЕОБХОДИМО ЗАЙТИ В СИСТЕМУ ЗАНОВО';
         RAISE LPB_EXCEPT;
      END IF;
      LP_DTODB := TREC_ODBDAY.ODB_DATE;
      DBMS_SESSION.SET_CONTEXT('ODB_CTX_PRVLOCAL', 'LOCAL_DTODB', TO_CHAR(TREC_ODBDAY.ODB_DATE, 'DD.MM.YYYY'));

      LP_SORT := NVL(SYS_CONTEXT('ODB_CTX_PRVAUTO', 'TREAD_SORT'), 'ASC');
      IF LP_SORT = 'DESC' THEN
         LP_SORT := 'ORDER BY D.VRFDT DESC';
      ELSE
         LP_SORT := '';
      END IF;

      BEGIN
         LP_SQL := ' SELECT * FROM (SELECT /*+INDEX (D, ODBDOCDAY_IDX_DTODBIDBPDR)*/D.IDDOC FROM ODB.ODB$DOCDAY D
                                   WHERE D.DTODB = TO_DATE(''' ||TO_CHAR(LP_DTODB, 'DD.MM.YYYY') || ''', ''DD.MM.YYYY'') AND
                                         D.IDBPDR = 1 AND
                                         D.VRFIS = ''Y'' AND NOT EXISTS (SELECT 1 FROM ODB.ODB$DOCDAY_PRVPUT WHERE IDDOC = D.IDDOC) AND
                                         D.PRVDTODB IS NULL ' || LP_CRITERION || ' ' || LP_SORT|| ') ';
      EXCEPTION
         WHEN VALUE_ERROR THEN
            P_RETCODE := -6;
            P_MESSAGE := 'СЛИШКОМ МНОГО УСЛОВИЙ ДЛЯ ПРАВИЛА';
            RAISE LPB_EXCEPT;
      END;

      LP_BEGIN := SYSDATE;
      OPEN CUR_DOCS4PRV FOR LP_SQL;
      LOOP
         FETCH CUR_DOCS4PRV INTO LP_IDDOC;
         EXIT WHEN CUR_DOCS4PRV%NOTFOUND;
         IF NVL(TO_NUMBER(SYS_CONTEXT('ODB_CTX_PRVAUTO', 'TREAD_STOPING')), 0) = 1 THEN
            P_RETCODE := -1;
            EXIT;
         END IF;

         IF ODB_GET_DOC_PRVSTATUS(LP_IDDOC) = 'N' THEN --ПРОВЕРКА СТАТУСА НЕПРОВЕДЕН(ИСКЛЮЧАЮТСЯ РЕПРОВЕДЕННЫЕ)
            SAVEPOINT PRV_IDDOC_AUTO;

            --ЗАПУСКАЕМ ПРОВОДКУ
            BEGIN
               LP_RET := 'Y';
               LP_MESS := 'БЛОКИРУЕМ ДОКУМЕНТ ID='||TO_CHAR(LP_IDDOC)||' ДЛЯ ПРОВЕДЕНИЯ';
               SELECT IDDOC INTO LP_IDDOC
                  FROM ODB$DOCDAY
                  WHERE IDDOC = LP_IDDOC
                  FOR UPDATE NOWAIT;
            EXCEPTION
               WHEN NO_DATA_FOUND THEN
                  LP_MESS := LP_MESS || ' - ЗАПИСЬ НЕ НАЙДЕНА';
                  LP_RET := 'N';
               WHEN OTHERS THEN
                  IF SQLCODE = -54 THEN
                     LP_MESS := LP_MESS || ' - БЛОКИРОВАН ДРУГИМ ПОЛЬЗОВАТЕЛЕМ!!!';
                  ELSE
                     LP_MESS := LP_MESS || CHR(10) || SQLERRM;
                  END IF;
                  LP_RET := 'N';
            END;

            IF LP_RET = 'Y' THEN
               BEGIN
                  LP_MESS := 'ДОКУМЕНТ ID='||TO_CHAR(LP_IDDOC)||' ПЕРЕВОДИМ В ДОКУМЕНТЫ ДНЯ';
                  LP_RET := ODBDOC_SRVC.DAY_IDDOC(LP_IDDOC, LP_DTODB, 'P');
               EXCEPTION
                  WHEN OTHERS THEN
                     LP_MESS := LP_MESS || CHR(10) || SQLERRM;
                     LP_RET := 'N';
               END;
            END IF;

            IF LP_RET = 'Y' THEN
               BEGIN
                  LP_MESS := 'ВЫПОЛНЯЕМ ПРОВОДКУ ДОКУМЕНТА ID='||TO_CHAR(LP_IDDOC);
                  LP_RET := ODBDOC_SRVC.PRV_IDDOC(LP_IDDOC, LP_DTODB, 'P');
               EXCEPTION
                  WHEN OTHERS THEN
                     LP_MESS := LP_MESS || CHR(10) || SQLERRM;
                     LP_RET := 'N';
               END;
            END IF;

            IF LP_RET = 'Y' THEN
               LP_CNT_READY := LP_CNT_READY + 1;
               LP_MESS := 'УДАЛЯЕМ ИНФОРМАЦИЮ ЕСЛИ ДОКУМЕНТ БЫЛ ОТЛОЖЕН ID='||TO_CHAR(LP_IDDOC);
               DELETE FROM ODB$DOCDAY_PRVPUT WHERE IDDOC = LP_IDDOC;
            ELSIF LP_RET = 'E' THEN
               LP_CNT_ERR := LP_CNT_ERR + 1;
               LP_MESS := 'ЧИТАЕМ ОШИБКИ НА ДОКУМЕНТЕ ID='||TO_CHAR(LP_IDDOC)|| ' ДЛЯ ОТМЕНЫ ТРАНЗАКЦИИ';
               SELECT ERDOC, ERDMFO, ERDACNUM, ERKMFO, ERKACNUM
                  INTO LP_ERDOC, LP_ERDMFO, LP_ERDACNUM, LP_ERKMFO, LP_ERKACNUM
                  FROM ODB$DOCDAY
                  WHERE IDDOC = LP_IDDOC;

               ROLLBACK TO PRV_IDDOC_AUTO;

               LP_MESS := 'ЗАПИСЫВАЕМ ОШИБКИ НА ДОКУМЕНТЕ ID='||TO_CHAR(LP_IDDOC);
               UPDATE ODB$DOCDAY
                  SET ERDOC    = LP_ERDOC,
                      ERDMFO   = LP_ERDMFO,
                      ERDACNUM = LP_ERDACNUM,
                      ERKMFO   = LP_ERKMFO,
                      ERKACNUM = LP_ERKACNUM
                  WHERE IDDOC  = LP_IDDOC;
            ELSE /*N*/
               ROLLBACK TO PRV_IDDOC_AUTO;
               LP_CNT_SKIP := LP_CNT_SKIP + 1;
               INSERT INTO ODB.ODB$DOCPRVLOG(IDRULE, MOTHER_SESSID, DT, NUM_WORK, IDDOC, SKIP_MESS)
                  VALUES (P_IDRULE, LP_TREAD_MOTHER, SYSDATE, LP_NUM_WORK, LP_IDDOC, SUBSTR(LP_MESS, 1, 1000));
            END IF;

            IF MOD(LP_CNT_READY + LP_CNT_SKIP + LP_CNT_ERR, 1000) = 0 THEN
               INSERT INTO ODB.ODB$DOCPRVLOG(IDRULE, MOTHER_SESSID, DT, NUM_WORK, DOCS_READY, DOCS_SKIP, DOCS_ERROR)
                  VALUES (P_IDRULE, LP_TREAD_MOTHER, SYSDATE, LP_NUM_WORK, LP_CNT_READY, LP_CNT_SKIP, LP_CNT_ERR);
               LP_CNT_READY := 0;
               LP_CNT_SKIP := 0;
               LP_CNT_ERR := 0;
            END IF;
            COMMIT;
         END IF;
      END LOOP;

      IF MOD(LP_CNT_READY + LP_CNT_SKIP + LP_CNT_ERR, 1000) <> 0 OR LP_CNT_READY + LP_CNT_SKIP + LP_CNT_ERR = 0 THEN
         INSERT INTO ODB.ODB$DOCPRVLOG(IDRULE, MOTHER_SESSID, DT, NUM_WORK, DOCS_READY, DOCS_SKIP, DOCS_ERROR)
            VALUES (P_IDRULE, LP_TREAD_MOTHER, SYSDATE, LP_NUM_WORK, LP_CNT_READY, LP_CNT_SKIP, LP_CNT_ERR);
         COMMIT;
      END IF;
      CLOSE CUR_DOCS4PRV;

      --ПРОВЕРЯЕМ НАЛИЧИЕ МАТЕРИНСКОЙ СЕССИИ
      IF NOT NVL(DBMS_SESSION.IS_SESSION_ALIVE(LP_TREAD_MOTHER), FALSE) THEN
         P_RETCODE := -5;
         P_MESSAGE := 'РАЗРЫВ СОЕДИНЕНИЯ ОСНОВНОЙ СЕССИИ';
         RAISE LPB_EXCEPT;
      END IF;

      /*КАЖДЫЙ ЦИКЛ ВЫПОЛНЯЕТСЯ НЕ МЕНЬШЕ 15 СЕКУНД*/
      LP_TIMEOUT := (SYSDATE - LP_BEGIN) * 24 * 3600;
      IF LP_TIMEOUT < 15 THEN
         DBMS_LOCK.SLEEP(15 - LP_TIMEOUT);
      END IF;
   END LOOP;

   --ДЛЯ ОЧИСТКИ ПАМЯТИ, ВЫДЕЛЯЕМОЙ ПОД ГЛОБАЛЬНЫЙ КОНТЕКСТ
   CTX_CLEAR_CONTEXT(LP_CLIDENTIFIER);

   APP_ADDLOG(LP_CLIDENTIFIER || ': ' || P_MESSAGE, 'PRVDOC');
   RETURN LP_CNT_READY;

EXCEPTION
   WHEN LPB_EXCEPT THEN
      ROLLBACK;
      CTX_CLEAR_CONTEXT(LP_CLIDENTIFIER);
      APP_ADDLOG(LP_CLIDENTIFIER || ': ' || P_MESSAGE, 'PRVDOC');
      RETURN LP_CNT_READY;
   WHEN NO_DATA_FOUND THEN
      ROLLBACK;
      P_RETCODE := -99;
      P_MESSAGE := LP_MESS || ' - ЗАПИСЬ НЕ НАЙДЕНА';
      CTX_CLEAR_CONTEXT(LP_CLIDENTIFIER);
      APP_ADDLOG(LP_CLIDENTIFIER || ': ' || P_MESSAGE, 'PRVDOC');
      RETURN -99;
   WHEN OTHERS THEN
      ROLLBACK;
      P_RETCODE := -99;
      P_MESSAGE := LP_MESS || TO_CHAR(SQLCODE) || ' - ' || SQLERRM;
      CTX_CLEAR_CONTEXT(LP_CLIDENTIFIER);
      APP_ADDLOG(LP_CLIDENTIFIER || ': ' || P_MESSAGE, 'PRVDOC');
      RETURN -99;
END PF_START_RULE;

--ПРОЦЕДУРА ИНИЦИАЛИЗАЦИИ СЕССИИ АВТОПРОВОДКИ
PROCEDURE PP_SESSION_INIT(P_CONNECT_DISCONNECT INTEGER,
                          P_RETCODE OUT INTEGER,
                          P_ISAUTO IN ODB$PRVSESSIONS.IS_AUTO%TYPE DEFAULT 'Y') AS
PRAGMA AUTONOMOUS_TRANSACTION;
--P_CONNECT_DISCONNECT =
--      1    - CONNECT
--      0    - DISCONNECT
--  P_RETCODE =
--      1    - НОРМАЛЬНО
--     -1    - ПРЕВЫШЕН ЛИМИТ КОЛИЧЕСТВА ПОДКЛЮЧЕНИЙ
--  P_ISAUTO =
--      'Y'  - АВТОПРОВОДКА
--      'N'  - РУЧНАЯ ПРОВОДКА

   LF_MESS      VARCHAR2(2000);
--   LF_CNT       INTEGER := 0; --СЧЕТЧИК ДЕЙСТВУЮЩИХ СЕССИЙ
   LF_CURSESS   ODB$PRVSESSIONS.IDSESSION%TYPE;
   LB_EXIST     BOOLEAN := FALSE;
BEGIN
   LF_CURSESS := DBMS_SESSION.UNIQUE_SESSION_ID;

   P_RETCODE := 1;
   IF P_CONNECT_DISCONNECT = 1 THEN
      FOR TREC_SESS IN (SELECT PS.* FROM ODB$PRVSESSIONS PS)
      LOOP
         IF NVL(DBMS_SESSION.IS_SESSION_ALIVE(TRIM(TREC_SESS.IDSESSION)), FALSE) THEN
            IF LF_CURSESS = TRIM(TREC_SESS.IDSESSION) THEN
               LB_EXIST := TRUE;
            END IF;
            --ЕСЛИ СЕССИЯ ЖИВА
--            IF TREC_SESS.IS_AUTO = 'Y' THEN
--               LF_CNT := LF_CNT + 1;
--            END IF;
         ELSE
            CTX_CLEAR_CONTEXT(TREC_SESS.IDSESSION);
            LF_MESS := 'УДАЛЯЕМ ИНФОРМАЦИЮ ОБ ОТСУТСТВУЮЩЕЙ СЕССИИ';
            DELETE FROM ODB$PRVSESSIONS WHERE TRIM(IDSESSION) = TREC_SESS.IDSESSION;
         END IF;
      END LOOP;

--      IF P_ISAUTO = 'Y' AND LF_CNT = 8 THEN
--         P_RETCODE := -1;
--         RAISE LPB_EXCEPT;
--      ELSE
      IF NOT LB_EXIST THEN
         LF_MESS := 'РЕГИСТРИРУЕМ НОВОЕ ПОДКЛЮЧЕНИЕ';
         INSERT INTO ODB$PRVSESSIONS(IDSESSION, DS, IS_AUTO) VALUES(DBMS_SESSION.UNIQUE_SESSION_ID, SYSDATE, P_ISAUTO);
      END IF;
--      END IF;
   ELSE
      CTX_CLEAR_CONTEXT(LF_CURSESS);
      LF_MESS := 'УДАЛЯЕМ ИНФОРМАЦИЮ О СЕССИИ ПРИ ОТКЛЮЧЕНИИ';
      DELETE FROM ODB$PRVSESSIONS WHERE IDSESSION = LF_CURSESS;
   END IF;

   COMMIT;
EXCEPTION
   WHEN LPB_EXCEPT THEN
      ROLLBACK;
   WHEN OTHERS THEN
      LF_MESS := LF_MESS || ' ' || SQLERRM;
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20999, LF_MESS, TRUE);
END PP_SESSION_INIT;

--ВЫБРАННЫЕ ДОКУМЕНТЫ В ПАЧКУ
PROCEDURE PP_DOCS2PACK(P_PACK IN VARCHAR2) AS
PRAGMA AUTONOMOUS_TRANSACTION;

   LP_PACK        VARCHAR2(4);
   LP_PACK_N      INTEGER;
   LP_MESS        VARCHAR2(2000);
   TREC_DOCDAY    ODB.ODB$DOCDAY%ROWTYPE;
   LP_SKIP        BOOLEAN := FALSE;
BEGIN
   LP_PACK := LPAD(TRIM(P_PACK), 4, '0');
   IF P_PACK IS NULL THEN
      LP_MESS := 'НЕ УКАЗАН НОМЕР ПАЧКИ';
      RAISE LPB_EXCEPT;
   ELSIF LENGTH(LP_PACK) <> 4 THEN
      LP_MESS := 'НОМЕР ПАЧКИ ПРЕВЫШАЕТ 4 СИМВОЛА';
      RAISE LPB_EXCEPT;
   ELSIF LP_PACK = '0000' THEN
      LP_MESS := 'НЕВЕРНОЕ ЗНАЧЕНИЕ НОМЕРА ПАЧКИ';
      RAISE LPB_EXCEPT;
   ELSE
      BEGIN
         LP_PACK_N := TO_NUMBER(LP_PACK);
      EXCEPTION
         WHEN OTHERS THEN
            LP_MESS := 'УКАЗАННОЕ ЗНАЧЕНИЕ НОМЕРА ПАЧКИ НЕ ЧИСЛОВОЕ';
            RAISE LPB_EXCEPT;
      END;
   END IF;

   FOR TREC IN (SELECT IDDOC FROM ODB$TMP_DOC4PRV)
   LOOP
      LP_MESS := '';
      LP_SKIP := FALSE;
      BEGIN
         SELECT D.*  INTO TREC_DOCDAY
            FROM ODB$DOCDAY D
            WHERE D.IDDOC = TREC.IDDOC
            FOR UPDATE NOWAIT;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            LP_MESS := 'ДОКУМЕНТ НЕ НАЙДЕН';
            LP_SKIP := TRUE;
         WHEN OTHERS THEN
            LP_MESS := 'ДОКУМЕНТ ЗАБЛОКИРОВАН';
            LP_SKIP := TRUE;
      END;

      IF NOT LP_SKIP THEN
         IF TREC_DOCDAY.PRVDT IS NOT NULL THEN
            LP_MESS := 'ДОКУМЕНТ УЖЕ ПРОВЕДЕН';
            LP_SKIP := TRUE;
         ELSIF TREC_DOCDAY.VRFIS = 'N' THEN
            LP_MESS := 'ДОКУМЕНТ НЕ ВЕРИФИЦИРОВАН ДЛЯ ПРОВОДКИ';
            LP_SKIP := TRUE;
         END IF;
      END IF;

      IF LP_SKIP THEN
         UPDATE ODB$TMP_DOC4PRV SET MESS = LP_MESS
            WHERE IDDOC = TREC.IDDOC;
      ELSE
         BEGIN
            LP_MESS := 'ДОКУМЕНТУ ID=' || TO_CHAR(TREC.IDDOC) || ' УСТАНАВЛИВАЕМ НОМЕР ПАЧКИ ' || LP_PACK;
            UPDATE ODB$DOCDAY SET KPCKG = LP_PACK
               WHERE IDDOC = TREC.IDDOC;
         EXCEPTION
            WHEN OTHERS THEN
               LP_MESS := LP_MESS || ' - ' || SQLERRM;
               UPDATE ODB$TMP_DOC4PRV SET MESS = LP_MESS
                  WHERE IDDOC = TREC.IDDOC;
         END;
      END IF;

      COMMIT;
   END LOOP;

   DELETE FROM ODB$TMP_DOC4PRV WHERE MESS IS NULL;
   COMMIT;

EXCEPTION
   WHEN LPB_EXCEPT THEN
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20999, LP_MESS);
   WHEN OTHERS THEN
      LP_MESS := LP_MESS || ' ' || SQLERRM;
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20999, LP_MESS, TRUE);
END PP_DOCS2PACK;

FUNCTION PF_DOCTRAIN(P_IDDOC IN ODB$DOCDAY.IDDOC%TYPE)
   RETURN TYPE_DOCDAY_TBL PIPELINED IS

   LP_IDDOC     ODB$DOCDAY.IDDOC%TYPE;
   OUT_REC      TYPE_IDDOC_REC;
BEGIN
   --СПИСОК СЛЕДУЮЩИХ ДОКУМЕНТОВ
   OUT_REC.IDDOC := P_IDDOC;
   PIPE ROW (OUT_REC);
   LP_IDDOC := P_IDDOC;
   WHILE LP_IDDOC IS NOT NULL LOOP
      BEGIN
         SELECT IDDOCNEXT INTO LP_IDDOC
            FROM ODB$DOCDAY WHERE IDDOC = LP_IDDOC;
         OUT_REC.IDDOC := LP_IDDOC;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            LP_IDDOC := NULL;
      END;
      IF LP_IDDOC IS NOT NULL THEN
         PIPE ROW (OUT_REC);
      END IF;
   END LOOP;

   --СПИСОК ПРЕДЫДУЩИХ ДОКУМЕНТОВ
   LP_IDDOC := P_IDDOC;
   WHILE LP_IDDOC IS NOT NULL LOOP
      BEGIN
         SELECT IDDOCPREV INTO LP_IDDOC
            FROM ODB$DOCDAY WHERE IDDOC = LP_IDDOC;
            OUT_REC.IDDOC := LP_IDDOC;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            LP_IDDOC := NULL;
      END;
      IF LP_IDDOC IS NOT NULL THEN
         PIPE ROW (OUT_REC);
      END IF;
   END LOOP;

   RETURN;
END PF_DOCTRAIN;

--ПРОЦЕДУРА ЗАПИСИ ПРОТОКОЛА ПЕЧАТИ
PROCEDURE PP_DOCS2PRNLOG IS
PRAGMA AUTONOMOUS_TRANSACTION;

   LP_MESS       VARCHAR2(2000);
BEGIN
   INSERT INTO ODB$DOCSPRNLOG(IDDOC)
      SELECT IDDOC FROM ODB$TMP_DOC4PRV
      WHERE MESS IS NULL;
   COMMIT;
EXCEPTION
   WHEN NO_DATA_FOUND THEN
      ROLLBACK;
   WHEN OTHERS THEN
      LP_MESS := LP_MESS || ' ' || SQLERRM;
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20999, LP_MESS, TRUE);
END PP_DOCS2PRNLOG;

PROCEDURE PP_$DOCSBRAK(P_REASON IN VARCHAR2,
                       P_ENDTRAN IN BOOLEAN) IS
--P_REASON = #UNVERIFY_OPERU# (СНЯТЬ АКЦЕПТ КОНТРОЛЁРА ОПЕРУ)
--ИНАЧЕ P_REASON первые 9 символов -  МАСКА КОДА БРАКОВКИ = #ERDOCxx#,
-- где xx-код браковки, если маска не задана, то код браковки = 'C6';

   LP_MESS       VARCHAR2(2000);
   TREC_DOCDAY   ODB$DOCDAY%ROWTYPE;
   LP_SKIP       BOOLEAN;
   LP_CNT        INTEGER;
   L_REASON      VARCHAR2(2000):= P_REASON;
   LP_RET        CHAR(1) := 'Y';

BEGIN
   IF TRIM(L_REASON) IS NULL THEN
      LP_MESS := 'НЕ УКАЗАНА ПРИЧИНА БРАКОВКИ ВЫБРАННЫХ ДОКУМЕНТОВ';
      RAISE LPB_EXCEPT;
   END IF;

   FOR TREC IN (SELECT IDDOC FROM ODB$TMP_DOC4PRV)
   LOOP
      LP_MESS := '';
      LP_SKIP := FALSE;
      BEGIN
         SELECT D.*  INTO TREC_DOCDAY
            FROM ODB$DOCDAY D
            WHERE D.IDDOC = TREC.IDDOC
            FOR UPDATE NOWAIT;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            LP_MESS := 'ДОКУМЕНТ НЕ НАЙДЕН';
            LP_SKIP := TRUE;
         WHEN OTHERS THEN
            LP_MESS := 'ДОКУМЕНТ ЗАБЛОКИРОВАН';
            LP_SKIP := TRUE;
      END;

      /*+++LMA 24.02.2012 ПРОВЕДЕННЫЕ ДОКУМЕНТЫ СНАЧАЛА СНИМАЕМ С ПРОВОДКИ*/
      ODBDOC_SRVC.SET_CHK_DTPRV('N');
      IF LP_SKIP THEN NULL;
      ELSE
         IF TREC_DOCDAY.PRVDTODB IS NOT NULL THEN
            BEGIN
               LP_MESS := 'ДОКУМЕНТ ID='||TO_CHAR(TREC.IDDOC)||' СНИМАЕМ ПРОВОДКУ В ДОКУМЕНТЫ ДНЯ';
               LP_RET := ODB.ODBDOC_SRVC.LGW$PRV_IDDOC(TREC.IDDOC, TREC_DOCDAY.PRVDTODB, 'R');
            EXCEPTION
               WHEN OTHERS THEN
                  LP_MESS := LP_MESS || CHR(10) || SQLERRM;
                  LP_SKIP := TRUE;
            END;
         END IF;
      END IF;

      IF LP_SKIP THEN NULL;
      ELSE
         IF TREC_DOCDAY.DAYDTODB IS NOT NULL AND LP_RET = 'Y' THEN
            BEGIN
               LP_MESS := 'ДОКУМЕНТ ID='||TO_CHAR(TREC.IDDOC)||' УБИРАЕМ ИЗ ДОКУМЕНТОВ ДНЯ';
               LP_RET := ODB.ODBDOC_SRVC.LGW$DAY_IDDOC(TREC.IDDOC, TREC_DOCDAY.DAYDTODB, 'R');
               IF LP_RET <> 'Y' THEN
                  LP_SKIP := TRUE;
               END IF;
            EXCEPTION
               WHEN OTHERS THEN
                  LP_MESS := LP_MESS || CHR(10) || SQLERRM;
                  LP_SKIP := TRUE;
            END;
         END IF;
      END IF;
      ODBDOC_SRVC.SET_CHK_DTPRV('Y');
      /*---LMA 24.02.2012 ПРОВЕДЕННЫЕ ДОКУМЕНТЫ СНАЧАЛА СНИМАЕМ С ПРОВОДКИ*/

      IF LP_SKIP THEN NULL;
      ELSE
         IF TREC_DOCDAY.VRFIS = 'N' THEN
            LP_MESS := 'ДОКУМЕНТ НЕ ВЕРИФИЦИРОВАН ДЛЯ ПРОВОДКИ';
            LP_SKIP := TRUE;
/*         ELSIF TREC_DOCDAY.SGNDOC = 'O' AND L_REASON <> '#UNVERIFY_OPERU#' THEN
            LP_MESS := 'ВНИМАНИЕ!!! ПОПЫТКА ЗАБРАКОВАТЬ ОТВЕТНЫЙ ДОКУМЕНТ. ОПЕРАЦИЯ НЕДОПУСТИМА.';
            LP_SKIP := TRUE;*/
         END IF;
      END IF;

      IF LP_SKIP THEN
         UPDATE ODB$TMP_DOC4PRV SET MESS = LP_MESS
            WHERE IDDOC = TREC.IDDOC;
      ELSE
         BEGIN
            IF L_REASON = '#UNVERIFY_OPERU#' THEN
               --СНЯТЬ АКЦЕПТ КОНТРОЛЕРА ОПЕРУ
               LP_MESS := 'ОПРЕДЕЛЯЕМ НАЛИЧИЕ ДОКУМЕНТА ID=' || TO_CHAR(TREC.IDDOC) || ' НА КОНТРОЛЕ ОПЕРУ';
               SELECT /*+INDEX(DC DOCCTRLST_IDX_IDCTRLSTEP)*/COUNT(*) INTO LP_CNT
                  FROM ODB$DOCCTRLST DC
                  WHERE DC.IDDOC = TREC.IDDOC AND
                        DC.IDCTRLSGN = 3;

               IF LP_CNT = 0 THEN
                  LP_MESS := 'ДОКУМЕНТ НЕ ПРОХОДИЛ КОНТРОЛЬ';
                  UPDATE ODB$TMP_DOC4PRV SET MESS = LP_MESS
                     WHERE IDDOC = TREC.IDDOC;
               ELSE
                  LP_MESS := 'СНИМАЕМ ПРИЗНАК ВЕРИФИКАЦИИ С ДОКУМЕНТА ID=' || TO_CHAR(TREC.IDDOC);
                  UPDATE ODB$DOCDAY
                     SET VRFIS = 'N'
                     WHERE IDDOC = TREC.IDDOC;
               END IF;
            ELSE
               --ЗАБРАКОВАТЬ
               LP_MESS := 'СНИМАЕМ ПРИЗНАК ВЕРИФИКАЦИИ С ДОКУМЕНТА ID=' || TO_CHAR(TREC.IDDOC);
               DECLARE
                 L_ERDOC CHAR(2) := 'C6';
               BEGIN
                  --#ERDOCxx#, где xx-код браковки
                  IF SUBSTR(L_REASON,1,6)='#ERDOC' AND
                     SUBSTR(L_REASON,9,1)='#' THEN
                    L_ERDOC := SUBSTR(L_REASON,7,2);
                    L_REASON := SUBSTR(L_REASON,10);
                  END IF;

                  IF TREC_DOCDAY.IDSYS <> 'SEB' AND TREC_DOCDAY.VRFIS = 'Y' THEN
                     UPDATE ODB$DOCDAY
                        SET ERDOC = L_ERDOC,
                            VRFIS = 'N'
                        WHERE IDDOC = TREC.IDDOC;
                  END IF;
               END;
               LP_MESS := 'ЗАПИСЫВАЕМ ПРИЧИНУ БРАКОВКИ';
               INSERT INTO ODB.ODB$DOCDAY_PRVLOG(IDDOC, PRVDT, PRVDTODB, ERRTEXT)
               VALUES(TREC_DOCDAY.IDDOC, TREC_DOCDAY.PRVDT, TREC_DOCDAY.PRVDTODB, L_REASON);
            END IF;

         EXCEPTION
            WHEN OTHERS THEN
               LP_MESS := LP_MESS || ' - ' || SQLERRM;
               UPDATE ODB$TMP_DOC4PRV SET MESS = LP_MESS
                  WHERE IDDOC = TREC.IDDOC;
         END;
      END IF;
      IF P_ENDTRAN THEN
         COMMIT;
      END IF;
   END LOOP;

EXCEPTION
   WHEN LPB_EXCEPT THEN
      IF P_ENDTRAN THEN
         ROLLBACK;
      END IF;
      RAISE_APPLICATION_ERROR(-20999, LP_MESS);
   WHEN OTHERS THEN
      LP_MESS := LP_MESS || ' ' || SQLERRM;
      IF P_ENDTRAN THEN
         ROLLBACK;
      END IF;
      RAISE_APPLICATION_ERROR(-20999, LP_MESS, TRUE);
END PP_$DOCSBRAK;


--ПРОЦЕДУРА БРАКОВКИ ВЫБРАННЫХ ДОКУМЕНТОВ
PROCEDURE PP_DOCSBRAK(P_REASON IN VARCHAR2) IS
PRAGMA AUTONOMOUS_TRANSACTION;
--P_REASON = #UNVERIFY_OPERU# (СНЯТЬ АКЦЕПТ КОНТРОЛЁРА ОПЕРУ)
   LP_MESS       VARCHAR2(2000);
   TREC_DOCDAY   ODB$DOCDAY%ROWTYPE;
   LP_SKIP       BOOLEAN;
   LP_CNT        INTEGER;
BEGIN
   IF TRIM(P_REASON) IS NULL THEN
      LP_MESS := 'НЕ УКАЗАНА ПРИЧИНА БРАКОВКИ ВЫБРАННЫХ ДОКУМЕНТОВ';
      RAISE LPB_EXCEPT;
   END IF;

   FOR TREC IN (SELECT IDDOC FROM ODB$TMP_DOC4PRV)
   LOOP
      LP_MESS := '';
      LP_SKIP := FALSE;
      BEGIN
         SELECT D.*  INTO TREC_DOCDAY
            FROM ODB$DOCDAY D
            WHERE D.IDDOC = TREC.IDDOC
            FOR UPDATE NOWAIT;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            LP_MESS := 'ДОКУМЕНТ НЕ НАЙДЕН';
            LP_SKIP := TRUE;
         WHEN OTHERS THEN
            LP_MESS := 'ДОКУМЕНТ ЗАБЛОКИРОВАН';
            LP_SKIP := TRUE;
      END;

--      IF NOT LP_SKIP THEN
--         LP_MESS := 'ПРОВЕРЯЕМ НЕ ОТЛОЖЕН ЛИ ДОКУМЕНТ';
--         SELECT COUNT(*) INTO LP_CNT
--            FROM ODB.ODB$DOCDAY_PRVPUT
--            WHERE IDDOC = TREC.IDDOC;
--         IF LP_CNT > 0 THEN
--            LP_MESS := 'ДОКУМЕНТ ОТЛОЖЕН. БРАКОВКА ЗАПРЕЩЕНА.';
--            LP_SKIP := TRUE;
--         END IF;
--      END IF;

      IF NOT LP_SKIP THEN
         IF TREC_DOCDAY.PRVDT IS NOT NULL THEN
            LP_MESS := 'ДОКУМЕНТ УЖЕ ПРОВЕДЕН';
            LP_SKIP := TRUE;
         ELSIF TREC_DOCDAY.VRFIS = 'N' THEN
            LP_MESS := 'ДОКУМЕНТ НЕ ВЕРИФИЦИРОВАН ДЛЯ ПРОВОДКИ';
            LP_SKIP := TRUE;
         ELSIF TREC_DOCDAY.SGNDOC = 'O' AND P_REASON <> '#UNVERIFY_OPERU#' THEN
            LP_MESS := 'ВНИМАНИЕ!!! ПОПЫТКА ЗАБРАКОВАТЬ ОТВЕТНЫЙ ДОКУМЕНТ. ОПЕРАЦИЯ НЕДОПУСТИМА.';
            LP_SKIP := TRUE;
         END IF;
      END IF;

      IF LP_SKIP THEN
         UPDATE ODB$TMP_DOC4PRV SET MESS = LP_MESS
            WHERE IDDOC = TREC.IDDOC;
      ELSE
         BEGIN
            IF P_REASON = '#UNVERIFY_OPERU#' THEN
               --СНЯТЬ АКЦЕПТ КОНТРОЛЕРА ОПЕРУ
               LP_MESS := 'ОПРЕДЕЛЯЕМ НАЛИЧИЕ ДОКУМЕНТА ID=' || TO_CHAR(TREC.IDDOC) || ' НА КОНТРОЛЕ ОПЕРУ';
               SELECT /*+INDEX(DC DOCCTRLST_IDX_IDCTRLSTEP)*/COUNT(*) INTO LP_CNT
                  FROM ODB$DOCCTRLST DC
                  WHERE DC.IDDOC = TREC.IDDOC AND
                        DC.IDCTRLSGN = 3;

               IF LP_CNT = 0 THEN
                  LP_MESS := 'ДОКУМЕНТ НЕ ПРОХОДИЛ КОНТРОЛЬ';
                  UPDATE ODB$TMP_DOC4PRV SET MESS = LP_MESS
                     WHERE IDDOC = TREC.IDDOC;
               ELSE
                  LP_MESS := 'СНИМАЕМ ПРИЗНАК ВЕРИФИКАЦИИ С ДОКУМЕНТА ID=' || TO_CHAR(TREC.IDDOC);
                  UPDATE ODB$DOCDAY
                     SET VRFIS = 'N'
                     WHERE IDDOC = TREC.IDDOC;
               END IF;
            ELSE
               --ЗАБРАКОВАТЬ
               LP_MESS := 'СНИМАЕМ ПРИЗНАК ВЕРИФИКАЦИИ С ДОКУМЕНТА ID=' || TO_CHAR(TREC.IDDOC);
               UPDATE ODB$DOCDAY
                  SET ERDOC = 'C6', VRFIS = 'N'
                  WHERE IDDOC = TREC.IDDOC;

               LP_MESS := 'ЗАПИСЫВАЕМ ПРИЧИНУ БРАКОВКИ';
               INSERT INTO ODB.ODB$DOCDAY_PRVLOG(IDDOC, PRVDT, PRVDTODB, ERRTEXT)
               VALUES(TREC_DOCDAY.IDDOC, TREC_DOCDAY.PRVDT, TREC_DOCDAY.PRVDTODB, P_REASON);
            END IF;

         EXCEPTION
            WHEN OTHERS THEN
               LP_MESS := LP_MESS || ' - ' || SQLERRM;
               UPDATE ODB$TMP_DOC4PRV SET MESS = LP_MESS
                  WHERE IDDOC = TREC.IDDOC;
         END;
      END IF;

      COMMIT;
   END LOOP;

EXCEPTION
   WHEN LPB_EXCEPT THEN
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20999, LP_MESS);
   WHEN OTHERS THEN
      LP_MESS := LP_MESS || ' ' || SQLERRM;
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20999, LP_MESS, TRUE);
END PP_DOCSBRAK;

--ПРОЦЕДУРА РАЗРЕШЕНИЯ ПРОВОДКИ НАЧАЛЬНЫХ ДОКУМЕНТОВ
PROCEDURE PP_NDOCS_PERMIT IS
PRAGMA AUTONOMOUS_TRANSACTION;

   LP_MESS       VARCHAR2(2000);
   TREC_DOCDAY   ODB$DOCDAY%ROWTYPE;
   LP_SKIP       BOOLEAN;
   LP_DTODB      ODBCNFDAY.ODB_DATE%TYPE;
BEGIN
   LP_MESS := 'ОПРЕДЕЛЯЕМ ТЕКУЩИЙ ОПЕРАЦИОННЫЙ ДЕНЬ БАНКА';
   SELECT D.ODB_DATE INTO LP_DTODB
       FROM ODBCNFDAY D
       WHERE D.ODB_STATE = 'O';

   FOR TREC IN (SELECT IDDOC FROM ODB$TMP_DOC4PRV)
   LOOP
      LP_SKIP := FALSE;
      BEGIN
         LP_MESS := 'ПРОВЕРЯЕМ ДОКУМЕНТ ID='||TO_CHAR(TREC.IDDOC);
         SELECT D.* INTO TREC_DOCDAY
            FROM ODB$DOCDAY D
            WHERE D.IDDOC = TREC.IDDOC;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            LP_MESS := LP_MESS || '. ДОКУМЕНТ НЕ НАЙДЕН';
            LP_SKIP := TRUE;
      END;

      IF NOT LP_SKIP THEN
         IF TREC_DOCDAY.SGNDOC <> 'N' THEN
            LP_MESS := LP_MESS || '. ДОКУМЕНТ НЕ НАЧАЛЬНЫЙ.';
            LP_SKIP := TRUE;
         ELSIF TREC_DOCDAY.ERDOC <> 'R1' THEN
            LP_MESS := LP_MESS || '. ДОКУМЕНТ НЕ БЛОКИРОВАН (R1 ОТСУТСТВУЕТ).';
            LP_SKIP := TRUE;
         END IF;
      END IF;

      IF LP_SKIP THEN
         UPDATE ODB$TMP_DOC4PRV SET MESS = LP_MESS
            WHERE IDDOC = TREC.IDDOC;
      ELSE
         BEGIN
            LP_MESS := 'ЗАНОСИМ ДОКУМЕНТ В СПРАВОЧНИК ПРАВИЛ ПРОВОДКИ НАЧАЛЬНЫХ ДОКУМЕНТОВ. ';

--            INSERT INTO ODB$PRVRULENDOC(DACNUM, KACNUM, KMFO, TYPEIDSEP, KV, STS, IDDOC, DTTERM)
--               VALUES (TREC_DOCDAY.DACNUM,TREC_DOCDAY.KACNUM, TREC_DOCDAY.KMFO, 1, TREC_DOCDAY.KV, 1, TREC_DOCDAY.IDDOC, LP_DTODB);

            INSERT INTO ODB$PRVRULENDOC(DACNUM, KACNUM, KMFO, TYPEIDSEP, KV, STS, IDDOC, DTTERM)
            SELECT TREC_DOCDAY.DACNUM,TREC_DOCDAY.KACNUM, TREC_DOCDAY.KMFO, 1, TREC_DOCDAY.KV, 1, TREC_DOCDAY.IDDOC, LP_DTODB
            FROM DUAL WHERE NOT EXISTS (SELECT 1 FROM ODB$PRVRULENDOC t
                                        WHERE T.IDDOC = TREC_DOCDAY.IDDOC AND
                                              T.DTTERM = LP_DTODB);
         EXCEPTION
            WHEN OTHERS THEN
               IF SQLCODE = -1 THEN
                  UPDATE ODB$TMP_DOC4PRV SET MESS = 'ДОКУМЕНТ УЖЕ ПРИСУТСТВУЕТ В СПРАВОЧНИКЕ.'
                     WHERE IDDOC = TREC.IDDOC;
               ELSE
                  LP_MESS := LP_MESS || SQLERRM;
                  RAISE LPB_EXCEPT;
               END IF;
         END;
      END IF;
   END LOOP;

   DELETE FROM ODB$TMP_DOC4PRV WHERE MESS IS NULL;
   COMMIT;
EXCEPTION
   WHEN NO_DATA_FOUND THEN
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20999, LP_MESS || ' ЗАПИСЬ НЕ НАЙДЕНА');
   WHEN LPB_EXCEPT THEN
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20999, LP_MESS);
   WHEN OTHERS THEN
      LP_MESS := LP_MESS || ' ' || SQLERRM;
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20999, LP_MESS, TRUE);
END PP_NDOCS_PERMIT;

--ФУНКЦИЯ ОПРЕДЕЛЕНИЯ КОТЛОВЫХ СЧЕТОВ КРЕДИТНОЙ СИСТЕМЫ
FUNCTION PF_GET_CRDTBOILER(P_DTODB IN ODB$DOCDAY.DTVAL%TYPE) RETURN TBL_CRDT_BOILER_ACNUM PIPELINED IS
   LP_MESS     VARCHAR2(2000);
   OUT_REC     REC_CRDT_BOILER_ACNUM;
   LP_DTODB     ODB$DOCDAY.DTVAL%TYPE;
BEGIN
   LP_MESS := 'ОПРЕДЕЛЯЕМ ТЕКУЩИЙ ОПЕРАЦИОННЫЙ ДЕНЬ БАНКА';
   SELECT D.ODB_DATE INTO LP_DTODB
       FROM ODBCNFDAY D
       WHERE D.ODB_STATE = 'O';

   LP_MESS := 'ОПРЕДЕЛЯЕМ СПИСОК КОТЛОВЫХ СЧЕТОВ';
   FOR TREC IN (SELECT A.ACNUM FROM ODB$SYSACNT A
                   WHERE A.IDSYS = 'CRDT' AND
                         P_DTODB BETWEEN A.DS AND NVL(A.DF, P_DTODB) AND
                         A.KVACNT = '980' AND
                         A.KVOPER = '980' AND
                         EXISTS (SELECT N.IDSYSACNT
                                    FROM ODB.ODB$NMSYSACNT N
                                    WHERE N.IDSYSACNT = A.IDSYSACNT AND
                                          N.IDSYS = 'CRDT' AND
                                          N.KEYACNT IN ('CNS-BOILER_LOAN2',
                                                        'CNS-BOILER_LOAN3',
                                                        'CNS-BOILER_SI',
                                                        'CNS-BOILER_PRFT',
                                                        'CNS-BOILER_SD2',
                                                        'CNS-BOILER_SD3',
                                                        'CNS-BOILER_FU')
                                )
               )
   LOOP
      OUT_REC.ACNUM := TREC.ACNUM;
      PIPE ROW (OUT_REC);
   END LOOP;
   RETURN;
EXCEPTION
   WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20999, LP_MESS, TRUE);
END PF_GET_CRDTBOILER;

--ПРОЦЕДУРА ОТКЛАДЫВАНИЯ ДОКУМЕНТОВ
--P_PUT: 1 - ОТЛОЖИТЬ
--       0 - ОТМЕНИТЬ ОТЛОЖЕНИЕ

PROCEDURE PP_DOCSPUT(P_PUT IN INTEGER) IS
PRAGMA AUTONOMOUS_TRANSACTION;

   LP_MESS       VARCHAR2(2000);
   TREC_DOCDAY   ODB$DOCDAY%ROWTYPE;
   LP_SKIP       BOOLEAN;
BEGIN
   IF P_PUT IN (0, 1) THEN
      NULL;
   ELSE
      LP_MESS := 'НЕПРАВИЛЬНЫЙ ВХОДНОЙ ПАРАМЕТР. ДОЛЖЕН БЫТЬ 0 ИЛИ 1';
      RAISE LPB_EXCEPT;
   END IF;

   FOR TREC IN (SELECT IDDOC FROM ODB$TMP_DOC4PRV)
   LOOP
      LP_SKIP := FALSE;
      BEGIN
         LP_MESS := 'ПРОВЕРЯЕМ ДОКУМЕНТ ID='||TO_CHAR(TREC.IDDOC);
         SELECT D.* INTO TREC_DOCDAY
            FROM ODB$DOCDAY D
            WHERE D.IDDOC = TREC.IDDOC;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            LP_MESS := LP_MESS || '. ДОКУМЕНТ НЕ НАЙДЕН';
            LP_SKIP := TRUE;
      END;

      IF LP_SKIP THEN
         NULL;
      ELSE
         IF TREC_DOCDAY.PRVDT IS NOT NULL THEN
            LP_MESS := LP_MESS || '. ДОКУМЕНТ УЖЕ ПРОВЕДЕН.';
            LP_SKIP := TRUE;
         ELSIF NVL(TREC_DOCDAY.ERDOC,    '00') = '00' AND
               NVL(TREC_DOCDAY.ERDMFO,   '00') = '00' AND
               NVL(TREC_DOCDAY.ERDACNUM, '00') = '00' AND
               NVL(TREC_DOCDAY.ERKMFO,   '00') = '00' AND
               NVL(TREC_DOCDAY.ERKACNUM, '00') = '00' AND P_PUT = 1 THEN
            LP_MESS := LP_MESS || '. ДОКУМЕНТ НЕ СОДЕРЖИТ ОШИБКИ.';
            LP_SKIP := TRUE;
         END IF;
      END IF;

      IF LP_SKIP THEN
         --УДАЛЯЕМ ПРИЗНАК ОТЛОЖЕННОСТИ ЕСЛИ ОН ОСТАЛСЯ
         DELETE FROM ODB.ODB$DOCDAY_PRVPUT WHERE IDDOC = TREC.IDDOC;
      ELSE
         BEGIN
            IF P_PUT = 1 THEN
               LP_MESS := 'ЗАНОСИМ ЗАПИСЬ В ТАБЛИЦУ ОТЛОЖЕННЫХ ДОКУМЕНТОВ';
               INSERT INTO ODB.ODB$DOCDAY_PRVPUT(IDDOC) VALUES (TREC.IDDOC);
            ELSE
               LP_MESS := 'УДАЛЯЕМ ПРИЗНАК ОТЛОЖЕННОСТИ ДОКУМЕНТА';
               DELETE FROM ODB.ODB$DOCDAY_PRVPUT WHERE IDDOC = TREC.IDDOC;
            END IF;
         EXCEPTION
            WHEN OTHERS THEN
               IF SQLCODE = -1 THEN
                  LP_MESS := 'ДОКМЕНТ УЖЕ ОТЛОЖЕН РАНЕЕ.';
                  LP_SKIP := TRUE;
               ELSE
                  LP_MESS := LP_MESS || CHR(10) || SQLERRM;
                  LP_SKIP := TRUE;
               END IF;
         END;
      END IF;

      IF LP_SKIP THEN
         --ЗАПИСЫВАЕМ СООбЩЕНИЕ ОБ ОШИБКЕ
         UPDATE ODB$TMP_DOC4PRV SET MESS = LP_MESS
            WHERE IDDOC = TREC.IDDOC;
      ELSE
         DELETE FROM ODB$TMP_DOC4PRV WHERE IDDOC = TREC.IDDOC;
      END IF;

   END LOOP;
   COMMIT;
EXCEPTION
   WHEN NO_DATA_FOUND THEN
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20999, LP_MESS || ' ЗАПИСЬ НЕ НАЙДЕНА');
   WHEN LPB_EXCEPT THEN
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20999, LP_MESS);
   WHEN OTHERS THEN
      LP_MESS := LP_MESS || ' ' || SQLERRM;
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20999, LP_MESS, TRUE);
END PP_DOCSPUT;

PROCEDURE PP_DOCS2NEXTDAY(P_DT IN DATE) AS
PRAGMA AUTONOMOUS_TRANSACTION;

   LP_MESS     VARCHAR(2000);

BEGIN
   IF P_DT < ODB_SRVC.GET_DTODB() THEN
      LP_MESS := 'ДАТА НЕ ДОЛЖНА БЫТЬ МЕНЬШЕ ТЕКУЩЕЙ ДАТЫ ОДБ';
      RAISE LPB_EXCEPT;
   END IF;

   LP_MESS := 'ОПРЕДЕЛЯЕМ ДОКУМЕНТЫ, НЕ СОДЕРЖАЩИЕ ОШИБКУ "99"';
   UPDATE ODB$TMP_DOC4PRV T
      SET T.MESS = 'ДОКУМЕНТ НЕ СОДЕРЖИТ ОШИБКУ "99"'
      WHERE EXISTS (SELECT 1 FROM ODB$DOCDAY D
                       WHERE D.IDDOC = T.IDDOC AND
                             NVL(D.ERDOC, '00') <> '99');

   FOR TREC IN (SELECT IDDOC FROM ODB$TMP_DOC4PRV T WHERE T.MESS IS NULL)
   LOOP
      ODB_DOCCTRL_SRVC.SET_CHANGE(TREC.IDDOC);
      LP_MESS := 'СНИМАЕМ ПРИЗНАК "ГОТОВ К ПРОВОДКЕ" С ДОКУМЕНТА ID='||TO_CHAR(TREC.IDDOC);
      UPDATE ODB$DOCDAY D SET D.VRFIS = 'N'
         WHERE D.IDDOC = TREC.IDDOC;

      LP_MESS := 'УСТАНАВЛИВАЕМ ДАТУ ПРОВОДКИ ДЛЯ ДОКУМЕНТА ID='||TO_CHAR(TREC.IDDOC);
      UPDATE ODB$DOCDAY D SET D.DTODB = P_DT
         WHERE D.IDDOC = TREC.IDDOC;

      LP_MESS := 'ВОЗВРАЩАЕМ ПРИЗНАК "ГОТОВ К ПРОВОДКЕ" С ДОКУМЕНТА ID='||TO_CHAR(TREC.IDDOC);
      UPDATE ODB$DOCDAY D SET D.VRFIS = 'Y'
         WHERE D.IDDOC = TREC.IDDOC;
      ODB_DOCCTRL_SRVC.SET_CHANGE(0);
   END LOOP;

   LP_MESS := 'УДАЛЯЕМ ВРЕМЕННЫЕ ДАННЫЕ';
   DELETE FROM ODB$TMP_DOC4PRV T WHERE T.MESS IS NULL;
   COMMIT;

EXCEPTION
   WHEN NO_DATA_FOUND THEN
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20999, LP_MESS || ' ЗАПИСЬ НЕ НАЙДЕНА');
   WHEN LPB_EXCEPT THEN
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20999, LP_MESS);
   WHEN OTHERS THEN
      LP_MESS := LP_MESS || ' ' || SQLERRM;
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-20999, LP_MESS, TRUE);
END PP_DOCS2NEXTDAY;

END ODB_$DOCPRV_SRVC;
/
