CREATE TABLE P1 (
	A INTEGER NOT NULL,
	C INTEGER NOT NULL,
	D INTEGER NOT NULL
	,CONSTRAINT T1_PK_TREE PRIMARY KEY(C, A)
        ,ASSUMEUNIQUE(C)
);
PARTITION TABLE P1 ON COLUMN A;
CREATE TABLE P2 (
	A INTEGER NOT NULL,
	C INTEGER NOT NULL,
	D INTEGER NOT NULL
	,CONSTRAINT T2_PK_TREE PRIMARY KEY(C, A)
        ,ASSUMEUNIQUE(C)
);
PARTITION TABLE P2 ON COLUMN A;


CREATE TABLE R1 (
	A INTEGER NOT NULL,
	C INTEGER NOT NULL
);
CREATE TABLE R2 (
	A INTEGER NOT NULL,
	C INTEGER NOT NULL
);

CREATE INDEX R2AC ON R2 (A,C);
CREATE INDEX R2C ON R2 (C);
