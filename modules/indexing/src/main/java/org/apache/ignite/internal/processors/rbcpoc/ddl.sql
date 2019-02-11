CREATE TABLE RISK
(
  RISK_ID                LONG,
  TRADEIDENTIFIER        LONG,
  TRADEVERSION           LONG,
  BATCHKEY               LONG,
  VALUE2                 DOUBLE,
  PRIMARY KEY (RISK_ID)
) with "KEY_TYPE=RISK_KEY,CACHE_NAME=RISK_CACHE,VALUE_TYPE=RISK_VALUE";
CREATE INDEX RISK_TR_IDX ON RISK (TRADEIDENTIFIER, TRADEVERSION, BATCHKEY) INLINE_SIZE 82;
/*------------------------------------------------------------------------*/
/*------------------------------------------------------------------------*/
/*------------------------------------------------------------------------*/
CREATE TABLE TRADE
(
  TRADEIDENTIFIER        LONG,
  TRADEVERSION           LONG,
  BOOK                   VARCHAR(8),
  VALUE1                 VARCHAR(50),
  PRIMARY KEY (TRADEIDENTIFIER, TRADEVERSION)
) with "KEY_TYPE=TRADE_KEY,CACHE_NAME=TRADE_CACHE,VALUE_TYPE=TRADE_VALUE";
CREATE INDEX TRADE_BOOK_PK_IDX ON TRADE (BOOK, TRADEIDENTIFIER, TRADEVERSION) INLINE_SIZE 100;
/*------------------------------------------------------------------------*/
/*------------------------------------------------------------------------*/
/*------------------------------------------------------------------------*/
CREATE TABLE BATCH
(
  BATCHKEY              LONG,
  ISLATEST              BOOLEAN,
  PRIMARY KEY (BATCHKEY)
) with "KEY_TYPE=BATCH_KEY,CACHE_NAME=BATCH_CACHE,VALUE_TYPE=BATCH_VALUE,TEMPLATE=REPLICATED";
CREATE INDEX BATCH_IDX ON BATCH (BATCHKEY);
/*------------------------------------------------------------------------*/
/*------------------------------------------------------------------------*/
/*------------------------------------------------------------------------*/
CREATE TABLE BATCH_ONHEAP
(
  BATCHKEY              LONG,
  ISLATEST              BOOLEAN,
  PRIMARY KEY (BATCHKEY)
) with "KEY_TYPE=BATCH_KEY,CACHE_NAME=BATCH_ONHEAP_CACHE,VALUE_TYPE=BATCH_VALUE,TEMPLATE=ONHEAP";
CREATE INDEX BATCH_ONHEAP_IDX ON BATCH_ONHEAP (BATCHKEY);
/*------------------------------------------------------------------------*/
/*------------------------------------------------------------------------*/
/*------------------------------------------------------------------------*/
CREATE TABLE BATCH_TRADE_LOOKUP
(
  BATCHKEY              LONG,
  TRADEIDENTIFIER        LONG,
  TRADEVERSION           LONG,
  DUMMY_VALUE      INT,
  PRIMARY KEY (BATCHKEY, TRADEIDENTIFIER, TRADEVERSION)
) with "KEY_TYPE=BATCH_TRADE_LOOKUP_KEY,CACHE_NAME=BATCH_TRADE_LOOKUP_CACHE,VALUE_TYPE=BATCH_TRADE_LOOKUP_VALUE";
CREATE INDEX BT_LOOKUP_IDX_1 ON BATCH_TRADE_LOOKUP (BATCHKEY, TRADEIDENTIFIER, TRADEVERSION);
CREATE INDEX BT_LOOKUP_IDX_2 ON BATCH_TRADE_LOOKUP (TRADEIDENTIFIER, TRADEVERSION, BATCHKEY);
/*------------------------------------------------------------------------*/
/*------------------------------------------------------------------------*/
/*------------------------------------------------------------------------*/
CREATE TABLE BATCH_TRADE_FULL
(
  BATCHKEY              LONG,
  TRADEIDENTIFIER        LONG,
  TRADEVERSION           LONG,
  ISLATEST              BOOLEAN,
  BOOK                   VARCHAR(8),
  VALUE1                 VARCHAR(50),
  PRIMARY KEY (BATCHKEY, TRADEIDENTIFIER, TRADEVERSION)
) with "KEY_TYPE=BATCH_TRADE_FULL_KEY,CACHE_NAME=BATCH_TRADE_FULL_CACHE,VALUE_TYPE=BATCH_TRADE_FULL_VALUE";
CREATE INDEX BT_FULL_IDX_1 ON BATCH_TRADE_FULL (BATCHKEY, TRADEIDENTIFIER, TRADEVERSION);
CREATE INDEX BT_FULL_IDX_2 ON BATCH_TRADE_FULL (TRADEIDENTIFIER, TRADEVERSION, BATCHKEY);
CREATE INDEX BT_FULL_IDX_3 ON BATCH_TRADE_FULL (BOOK);