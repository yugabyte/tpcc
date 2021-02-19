DROP TABLE IF EXISTS WAREHOUSE;
CREATE TABLE WAREHOUSE (
  W_ID INT NOT NULL,
  W_YTD DECIMAL(12,2) NOT NULL,
  W_TAX DECIMAL(4,4) NOT NULL,
  W_NAME VARCHAR(10) NOT NULL,
  W_STREET_1 VARCHAR(20) NOT NULL,
  W_STREET_2 VARCHAR(20) NOT NULL,
  W_CITY VARCHAR(20) NOT NULL,
  W_STATE CHAR(2) NOT NULL,
  W_ZIP CHAR(9) NOT NULL,
  PRIMARY KEY (W_ID)
);

DROP TABLE IF EXISTS DISTRICT;
CREATE TABLE DISTRICT (
  D_W_ID INT NOT NULL REFERENCES WAREHOUSE (W_ID),
  D_ID INT NOT NULL,
  D_YTD DECIMAL(12,2) NOT NULL,
  D_TAX DECIMAL(4,4) NOT NULL,
  D_NEXT_O_ID INT NOT NULL,
  D_NAME VARCHAR(10) NOT NULL,
  D_STREET_1 VARCHAR(20) NOT NULL,
  D_STREET_2 VARCHAR(20) NOT NULL,
  D_CITY VARCHAR(20) NOT NULL,
  D_STATE CHAR(2) NOT NULL,
  D_ZIP CHAR(9) NOT NULL,
  PRIMARY KEY (D_W_ID,D_ID)
);

-- TODO: C_SINCE ON UPDATE CURRENT_TIMESTAMP,
DROP TABLE IF EXISTS CUSTOMER;
CREATE TABLE CUSTOMER (
  C_W_ID INT NOT NULL,
  C_D_ID INT NOT NULL,
  C_ID INT NOT NULL,
  C_DISCOUNT DECIMAL(4,4) NOT NULL,
  C_CREDIT CHAR(2) NOT NULL,
  C_LAST VARCHAR(16) NOT NULL,
  C_FIRST VARCHAR(16) NOT NULL,
  C_CREDIT_LIM DECIMAL(12,2) NOT NULL,
  C_BALANCE DECIMAL(12,2) NOT NULL,
  C_YTD_PAYMENT FLOAT NOT NULL,
  C_PAYMENT_CNT INT NOT NULL,
  C_DELIVERY_CNT INT NOT NULL,
  C_STREET_1 VARCHAR(20) NOT NULL,
  C_STREET_2 VARCHAR(20) NOT NULL,
  C_CITY VARCHAR(20) NOT NULL,
  C_STATE CHAR(2) NOT NULL,
  C_ZIP CHAR(9) NOT NULL,
  C_PHONE CHAR(16) NOT NULL,
  C_SINCE TIMESTAMP NOT NULL,
  C_MIDDLE CHAR(2) NOT NULL,
  C_DATA VARCHAR(500) NOT NULL,
  PRIMARY KEY (C_W_ID, C_D_ID, C_ID),
  CONSTRAINT C_FKEY_D FOREIGN KEY (C_W_ID, C_D_ID) REFERENCES DISTRICT (D_W_ID, D_ID)
);
CREATE INDEX IDX_CUSTOMER_NAME ON CUSTOMER (C_W_ID,C_D_ID,C_LAST,C_FIRST);

-- TODO: O_ENTRY_D  ON UPDATE CURRENT_TIMESTAMP
DROP TABLE IF EXISTS OORDER;
CREATE TABLE OORDER (
  O_W_ID INT NOT NULL,
  O_D_ID INT NOT NULL,
  O_ID INT NOT NULL,
  O_C_ID INT NOT NULL,
  O_CARRIER_ID INT DEFAULT NULL,
  O_OL_CNT DECIMAL(2,0) NOT NULL,
  O_ALL_LOCAL DECIMAL(1,0) NOT NULL,
  O_ENTRY_D TIMESTAMP NOT NULL,
  PRIMARY KEY (O_W_ID,O_D_ID,O_ID),
  UNIQUE (O_W_ID,O_D_ID,O_C_ID,O_ID),
  CONSTRAINT O_FKEY_C FOREIGN KEY (O_W_ID, O_D_ID, O_C_ID) REFERENCES CUSTOMER (C_W_ID, C_D_ID, C_ID)
);

DROP TABLE IF EXISTS NEW_ORDER;
CREATE TABLE NEW_ORDER (
  NO_W_ID INT NOT NULL,
  NO_D_ID INT NOT NULL,
  NO_O_ID INT NOT NULL,
  PRIMARY KEY (NO_W_ID,NO_D_ID,NO_O_ID),
  CONSTRAINT NO_FKEY_O FOREIGN KEY (NO_W_ID, NO_D_ID, NO_O_ID) REFERENCES OORDER (O_W_ID, O_D_ID, O_ID)
);

-- TODO: H_DATE ON UPDATE CURRENT_TIMESTAMP
DROP TABLE IF EXISTS HISTORY;
CREATE TABLE HISTORY (
  H_C_ID INT NOT NULL,
  H_C_D_ID INT NOT NULL,
  H_C_W_ID INT NOT NULL,
  H_D_ID INT NOT NULL,
  H_W_ID INT NOT NULL,
  H_DATE TIMESTAMP NOT NULL,
  H_AMOUNT DECIMAL(6,2) NOT NULL,
  H_DATA VARCHAR(24) NOT NULL,
  CONSTRAINT H_FKEY_C FOREIGN KEY (H_C_W_ID, H_C_D_ID, H_C_ID) REFERENCES CUSTOMER (C_W_ID, C_D_ID, C_ID),
  CONSTRAINT H_FKEY_D FOREIGN KEY (H_W_ID, H_D_ID) REFERENCES DISTRICT (D_W_ID, D_ID)
);

DROP TABLE IF EXISTS ITEM;
CREATE TABLE ITEM (
  I_ID INT NOT NULL,
  I_NAME VARCHAR(24) NOT NULL,
  I_PRICE DECIMAL(5,2) NOT NULL,
  I_DATA VARCHAR(50) NOT NULL,
  I_IM_ID INT NOT NULL,
  PRIMARY KEY (I_ID)
);

DROP TABLE IF EXISTS STOCK;
CREATE TABLE STOCK (
  S_W_ID INT NOT NULL REFERENCES WAREHOUSE (W_ID),
  S_I_ID INT NOT NULL REFERENCES ITEM (I_ID),
  S_QUANTITY DECIMAL(4,0) NOT NULL,
  S_YTD DECIMAL(8,2) NOT NULL,
  S_ORDER_CNT INT NOT NULL,
  S_REMOTE_CNT INT NOT NULL,
  S_DATA VARCHAR(50) NOT NULL,
  S_DIST_01 CHAR(24) NOT NULL,
  S_DIST_02 CHAR(24) NOT NULL,
  S_DIST_03 CHAR(24) NOT NULL,
  S_DIST_04 CHAR(24) NOT NULL,
  S_DIST_05 CHAR(24) NOT NULL,
  S_DIST_06 CHAR(24) NOT NULL,
  S_DIST_07 CHAR(24) NOT NULL,
  S_DIST_08 CHAR(24) NOT NULL,
  S_DIST_09 CHAR(24) NOT NULL,
  S_DIST_10 CHAR(24) NOT NULL,
  PRIMARY KEY (S_W_ID HASH, S_I_ID ASC)
);

DROP TABLE IF EXISTS ORDER_LINE;
CREATE TABLE ORDER_LINE (
  OL_W_ID INT NOT NULL,
  OL_D_ID INT NOT NULL,
  OL_O_ID INT NOT NULL,
  OL_NUMBER INT NOT NULL,
  OL_I_ID INT NOT NULL,
  OL_DELIVERY_D TIMESTAMP,
  OL_AMOUNT DECIMAL(6,2) NOT NULL,
  OL_SUPPLY_W_ID INT NOT NULL,
  OL_QUANTITY DECIMAL(2,0) NOT NULL,
  OL_DIST_INFO CHAR(24) NOT NULL,
  PRIMARY KEY (OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER),
  CONSTRAINT OL_FKEY_O FOREIGN KEY (OL_W_ID, OL_D_ID, OL_O_ID) REFERENCES OORDER (O_W_ID, O_D_ID, O_ID),
  CONSTRAINT OL_FKEY_S FOREIGN KEY (OL_SUPPLY_W_ID, OL_I_ID) REFERENCES STOCK (S_W_ID, S_I_ID)
);