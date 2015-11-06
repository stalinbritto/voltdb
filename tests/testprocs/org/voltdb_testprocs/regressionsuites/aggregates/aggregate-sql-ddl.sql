CREATE TABLE P1 (
  ID INTEGER DEFAULT '0' NOT NULL,
  DESC VARCHAR(300),
  CASH DECIMAL,
  NUM INTEGER,
  RATIO FLOAT,
  PRIMARY KEY (ID)
);

CREATE TABLE R1 (
  ID INTEGER DEFAULT '0' NOT NULL,
  DESC VARCHAR(300),
  CASH DECIMAL,
  NUM INTEGER,
  RATIO FLOAT,
  PRIMARY KEY (ID)
);

CREATE TABLE P2 (
  ID INTEGER DEFAULT '0' NOT NULL,
  DESC VARCHAR(300),
  CASH DECIMAL,
  NUM INTEGER,
  RATIO FLOAT,
  CONSTRAINT P2_PK_TREE PRIMARY KEY (ID)
);

CREATE TABLE R2 (
  ID INTEGER DEFAULT '0' NOT NULL,
  DESC VARCHAR(300),
  CASH DECIMAL,
  NUM INTEGER,
  RATIO FLOAT,
  CONSTRAINT R2_PK_TREE PRIMARY KEY (ID)
);

CREATE TABLE ENG3465 (
  EMPNUM   VARCHAR(3) NOT NULL,
  PNUM     VARCHAR(3) NOT NULL,
  HOURS    DECIMAL(5),
  UNIQUE(EMPNUM,PNUM)
);

CREATE TABLE ENG_9394 (
   CODE varchar(32 BYTES) NOT NULL,
   ANAME varchar(34 BYTES) NOT NULL,
   NUMCODE varchar(16 BYTES) NOT NULL,
   DT1 timestamp NOT NULL,
   DT2 timestamp NOT NULL,
   ACODE varchar(8 BYTES),
   PRIMARY KEY (CODE, NUMCODE, DT1, DT2)

);
PARTITION TABLE ENG_9394 ON COLUMN CODE;
CREATE INDEX ENG_9394_IDX2 ON ENG_9394 (DT2);
