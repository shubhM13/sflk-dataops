# Snowflake-Change-Management


A set of 2 procedures and a python tool has been developed to handle change management
tasks for the projects.

- edw_ops.ops.clone_db_objects
- edw_ops.ops.clone_db_objects_adv
- csv_to_json.ipynb

The aim is to be able to migrate the following Snowflake DB object types from source
environment to target environment:

1. Tables
2. Views
3. Stages
4. Functions
5. File Formats
6. Stored Procedures
7. Streams
8. Tasks

The procedures leverage the cloning capability provided by snowflake which allows to create
writable zero copy clones within the system.

The above 8 DB objects can be logically divided into two groups:

1. The ones for which the metadata is readily available in the INFORMATION_SCHEMA -
    Tables, Views, Procedures, Functions and Stages
2. The others for which it is not - File Formats, Streams and Tasks

For the former, the idea is to pick up the bits and pieces of information available in the
INFORMATION_SCHEMA and assimilate them into a functional DDL which captures all the
relevant details of the source object.

For the later, we either make use snowflake’s **GET_DDL()** or **SHOW<OBJECTS>** to get the
metadata about a specific DB objects and generate the DDL.

NOTE: These procedures cannot perform cross-schema object migration; rather it assumes
that the objects must be migrated between the corresponding schema in source and target.
Also, it does not create new schema in the target; rather assumes that it already exists.

## I. clone_db_objects

This procedure has been developed with the purpose to be able to do bulk object
migrations from:

1. Source DB to Target DB – Moves all the 8 DB object types within all four standard
    schemata viz. STG, INT, SEC, PRS.
2. A schema in source DB to the corresponding schema in target DB – Moves all the 8
    DB object types from source DB to the target DB existing within the specified
    schema.


3. A set of DB objects having a particular naming convention which is specific to a
    project – Moves all the 8 DB object types having the specified prefix from the source
    DB to the target DB.
4. A specific object type from source to the target – Moves all the DB objects belonging
    to the specified type from the source DB to the target DB irrespective of the schema
    they belong to.

### Argument List:

1. SOURCE_DB - Source Database name
2. TARGET_DB - Target Database name
3. OBJ_PATTERN - Project name (or any text pattern in general) which is matched as
    prefix against the DB object names.
4. SCHEMA_FILTER - A string specifying the schema to be moved (if empty or ‘ALL’, all
    the schema in source DB will be moved).
5. OBJECT_TYPE - A string specifying the DB type to be moved viz. ALL, TABLE, VIEW, FF,
    STAGES, SP, FUNCTION, STREAMS, TASK. These are the only valid argument values.
6. FIRST_RUN – A Boolean specifying whether the current migration is the first run, in
    that case all the DDLs are ‘CREATE <OBJECT> IF NOT EXISTS’ statements.
7. MOVE_TABLE_STRUCTURE_ONLY – A Boolean specifying whether we want to move
    table structures only and not source data references are required. If set to 0, the DB
    objects will be simply cloned as a zero copy.
8. PRESERVE_TARGET_TABLE_DATA – If the above argument is 1, this Boolean
    argument specifies whether we want to preserve the data existing the target DB. If
    set, it will create a backup table to store the original target table data and then
    migrate the new structure from source to the target.

## II. clone_db_objects_adv

This procedure has been developed to give the user flexibility to be able to move a specific
DB object rather than doing bulk migration of the entire DB or schema.

This is achieved by providing a JSON string having the list of objects to be moved categorized
by object types as JSON keys.

For ex.

{

"STG": {

"SCHEMA": "STG",

"TABLES": "ACCOUNT_ACCOUNT_NUMBER,BAK_STG_Y5PPLZA0",

"VIEWS": "COMPANY_CODE_SV",

"FF": "",

"STAGES": "",


#### "SP": "CLONE_DB_OBJECTS_BASIC,CLONE_DB_OBJECTS_ADVANCED",

#### "FUNCTIONS": " ",

#### "STREAMS": "STG_0APO_LOCNO_S,STG_0APO_LPROD_S",

#### "TASKS": "ELT_CONTROL_TABLE_TASK,GRP_1_EVENT_TRIGGER_TASK"

#### }

#### }

### Argument List:

1. SOURCE_DB - Source Database name
2. TARGET_DB - Target Database name
3. OBJ_PATTERN - Project name (or any text pattern in general) which is matched as
    prefix against the DB object names.
4. ADV_FILTER – JSON string specifying the schema name and the list of objects to be
    moved from that schema.

## III. csv_to_json.ipynb

This is a simple python tool to convert CSV to JSON string. It takes a CSV file name as input
and generates the JSON string.

The headers of the CSV file need to be as follows

```
SCHEMA TABLES VIEWS FF STAGES SP FUNCTIONS STREAMS TASKS
```

