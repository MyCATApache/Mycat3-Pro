ALTER SESSION SET CONTAINER = FREEPDB1;
SET DEFINE OFF;
WHENEVER SQLERROR EXIT SQL.SQLCODE

DECLARE
  v_count NUMBER;
BEGIN
  SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = 'DB1_0';
  IF v_count = 0 THEN
    EXECUTE IMMEDIATE 'CREATE USER DB1_0 IDENTIFIED BY demo_pw';
  END IF;
  SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = 'DB1_1';
  IF v_count = 0 THEN
    EXECUTE IMMEDIATE 'CREATE USER DB1_1 IDENTIFIED BY demo_pw';
  END IF;
END;
/

GRANT CONNECT, RESOURCE TO DB1_0;
GRANT CONNECT, RESOURCE TO DB1_1;
GRANT UNLIMITED TABLESPACE TO DB1_0;
GRANT UNLIMITED TABLESPACE TO DB1_1;

DECLARE
  TYPE ddl_tab IS TABLE OF VARCHAR2(4000);
  ddl ddl_tab := ddl_tab(
    q'[CREATE TABLE DB1_0.ORDERS_0 (
      "id" NUMBER(19) PRIMARY KEY,
      "user_id" NUMBER(19) NOT NULL,
      "amount" NUMBER(12,2) NOT NULL,
      "status" VARCHAR2(16) NOT NULL,
      "created" DATE NOT NULL
    )]',
    q'[CREATE TABLE DB1_0.ORDERS_1 (
      "id" NUMBER(19) PRIMARY KEY,
      "user_id" NUMBER(19) NOT NULL,
      "amount" NUMBER(12,2) NOT NULL,
      "status" VARCHAR2(16) NOT NULL,
      "created" DATE NOT NULL
    )]',
    q'[CREATE TABLE DB1_1.ORDERS_2 (
      "id" NUMBER(19) PRIMARY KEY,
      "user_id" NUMBER(19) NOT NULL,
      "amount" NUMBER(12,2) NOT NULL,
      "status" VARCHAR2(16) NOT NULL,
      "created" DATE NOT NULL
    )]',
    q'[CREATE TABLE DB1_1.ORDERS_3 (
      "id" NUMBER(19) PRIMARY KEY,
      "user_id" NUMBER(19) NOT NULL,
      "amount" NUMBER(12,2) NOT NULL,
      "status" VARCHAR2(16) NOT NULL,
      "created" DATE NOT NULL
    )]',
    q'[CREATE TABLE DB1_0.TENANT_ORDER_0 (
      "id" NUMBER(19) PRIMARY KEY,
      "tenant_id" VARCHAR2(32) NOT NULL,
      "note" VARCHAR2(128) NOT NULL,
      "created" DATE NOT NULL
    )]',
    q'[CREATE TABLE DB1_1.TENANT_ORDER_1 (
      "id" NUMBER(19) PRIMARY KEY,
      "tenant_id" VARCHAR2(32) NOT NULL,
      "note" VARCHAR2(128) NOT NULL,
      "created" DATE NOT NULL
    )]',
    q'[CREATE TABLE DB1_0."dict" (
      "id" NUMBER(19) PRIMARY KEY,
      "name" VARCHAR2(64) NOT NULL
    )]',
    q'[CREATE TABLE DB1_1."dict" (
      "id" NUMBER(19) PRIMARY KEY,
      "name" VARCHAR2(64) NOT NULL
    )]',
    q'[CREATE TABLE DB1_0.USER_INFO (
      "id" NUMBER(19) PRIMARY KEY,
      "name" VARCHAR2(64) NOT NULL,
      "profile" VARCHAR2(255) NOT NULL
    )]'
  );
BEGIN
  FOR i IN 1 .. ddl.COUNT LOOP
    BEGIN
      EXECUTE IMMEDIATE ddl(i);
    EXCEPTION
      WHEN OTHERS THEN
        IF SQLCODE != -955 THEN
          RAISE;
        END IF;
    END;
  END LOOP;
END;
/

COMMIT;
EXIT
