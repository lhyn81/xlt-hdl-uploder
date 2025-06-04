This is the task description for the project.
Read this to generate code for the project.

# Project Name
`Xlt db uploader`

# Project Language
+ python 3.10.10
+ pyside6

# Overall Project Description
This project is to:
+ query and display the data from sqilite database `record.db`.
+ upload the selected data to the remote ORACLE database.
+ query and sisplay the data from the remote ORACLE database.

# TASK
create a pyside6 application containing the following:
+ a radio button to select `local` or `remote` datasource. `Local` database is `record.db`, and `remote` database is `Data Source=10.142.9.101:1521/ORCL;User ID=C##HDLTZ;Password=HDLTZ`.
+ a search condition area to let user select `date` and `id` to query the data from the database.
+ a table to display the data from the database.
+ `local` data table structure is defined in  `record.db`, understand it.
+ `remote` data table structure is defined as following:
  ```
  CREATE TABLE LDCLJ_TIAOMA 
   (	GID VARCHAR2(32) DEFAULT sys_guid() NOT NULL ENABLE, 
	MRLCODE VARCHAR2(50), 
	CREATE_ID VARCHAR2(32) DEFAULT 'ldclj', 
	CREATE_DATE DATE DEFAULT SYSDATE NOT NULL ENABLE, 
	MODIFY_ID VARCHAR2(32) DEFAULT 'ldclj', 
	MODIFY_DATE DATE, 
	FLAG NUMBER(1,0), 
	BEARINGMOMENTUM VARCHAR2(50), 
	GASKETTHICKNESS VARCHAR2(50), 
	DRIVENGEAR VARCHAR2(50), 
	RADIALRUNOUTTOOTHSIDE VARCHAR2(50), 
	RADIALRUNOUTNONTOOTHSIDE VARCHAR2(50), 
	FACERUNOUTTOOTHSIDE VARCHAR2(50), 
	FACERUNOUTNONTOOTHSIDE VARCHAR2(50), 
	DRIVENGEARTOOTHSIDE VARCHAR2(50), 
	DRIVENGEARNONTOOTHSIDE VARCHAR2(50), 
	UDA1 VARCHAR2(255), 
	UDA2 VARCHAR2(255), 
	UDA3 VARCHAR2(255), 
	UDA4 VARCHAR2(255), 
	UDA5 VARCHAR2(255), 
	UDA6 VARCHAR2(255), 
	UDA7 VARCHAR2(255), 
	UDA8 VARCHAR2(255), 
	UDA9 VARCHAR2(255), 
	UDA10 VARCHAR2(255)
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 8192 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE MESTAR_DATA  ENABLE ROW MOVEMENT ;
  ```
+ user can give `date range` or `id` to query the data from the database.
+ if `local` database is selected, user can also upload the selected data to the remote database.
Note: You can suppose the mapping fields of `local` database and `remote` database as will, I will fix the mapping later. Implement the upload function first.

