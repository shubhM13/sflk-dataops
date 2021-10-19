create or replace procedure edw_ops.ops.clone_db_objects(SOURCE_DB VARCHAR, TARGET_DB VARCHAR, OBJ_PATTERN VARCHAR, SCHEMA_FILTER VARCHAR, OBJECT_TYPE VARCHAR, FIRST_RUN BOOLEAN, MOVE_TABLE_STRUCTURE_ONLY BOOLEAN, PRESERVE_TARGET_TABLE_DATA BOOLEAN, TARGET_WH VARCHAR)
returns variant not null
language javascript
EXECUTE AS caller
as
$$
//------------------------------------------------------------------------------------------------------------------------//
//- Stored Procedure Name 	: clone_db_objects 
//- Description 			: This script will clone the entire content of 1 DB into enother, assuming that source and target were created by infront to propagate standard level of permissions 
//- Script Author 			: Shubham Mishra <smishra.shubhammishra@gmail.com>       
//- Import Notes			: Script assumes existance of all schemas in source and target as well, and that security model is already deployed on target.
//							: None correct objects like views that cannot be compiled are not copied to target
//                          : Script assumes that the source db warehouse for tasks is usable in target db also
//                          : For preserving target table data, the procedure first stores the data in a backup table as a clone and then migrates the structure.
//                          : The script takes care of tasks-to-task dependency
//                          : The script does not take care of view-to-view dependency currently
//
//- Parameters              : 1) <SOURCE_DB> - Source Database name
//                            2) <TARGET_DB> - Target Database name
//                            3) <OBJECT_PATTERN> - Project name (or any text pattern in general) which is matched as prefix against the DB object names
//							  4) <SCHEMA_FILTER> - A string specifying the schema to be moved (if empty, all the schema in src_db will be moved)
//                            5) <OBJECT_TYPE> - A string specifying the DB type to be moved viz. ALL, TABLE, VIEW, FF, STAGES, SP, FUNCTION, STREAMS, TASK. These are the only valid argument values. 
//                            6) <FIRST_RUN> - A Boolean specifying whether the current migration is the first run, in that case all the DDLs are ‘CREATE <OBJECT> IF NOT EXISTS’ statements.
//                            7) <MOVE_TABLE_STRUCTURE_ONLY> - A Boolean specifying whether we want to move table structures only and not source data references are required. If set to 0, the DB objects will be simply cloned as a zero copy.
//                            8) <PRESERVE_TARGET_TABLE_DATA> - If the above argument is 1, this Boolean argument specifies whether we want to preserve the data existing the target DB. If set, it will create a backup table to store the original target table data and then migrate the new structure from source to the target.
//
//- Returns                 : Execution log in the form of JSON
//
//------------------------------------------------------------------------------------------------------------------------//
// This array will contain all the ddls.
var array_of_rows = [];
// This variable will hold a JSON data structure that we can return as a VARIANT.
// This will contain ALL the rows in a single "value".
var table_as_json = {};
//execution report holder
var report_rows = [];

//For book-keeping the count of db objects 
var table_num = 0;
var view_num = 0;
var ff_num = 0;
var stage_num = 0;
var sp_num = 0;
var fun_num = 0;
var stream_num = 0;
var task_num = 0;
var seq_num = 0;

var res_message = "";

var sql_comand = "";
// Read the schemas in the environment and arrange them in standard dependency order
// TODO - STGHR, INTHR, PRSHR - Make prefix matching approach
var schema_names = [];

if (SCHEMA_FILTER.trim() === null || SCHEMA_FILTER.trim() === '') {

    sql_command = "SELECT SCHEMA_NAME, CASE  WHEN  regexp_like (schema_name,'STG[a-zA-Z]*') THEN 1 WHEN REGEXP_LIKE  (SCHEMA_NAME,'[INT|CORE][a-zA-Z]*') THEN 2 WHEN SCHEMA_NAME in ('SEC') THEN 3 WHEN REGEXP_LIKE (SCHEMA_NAME,'PRS[a-zA-Z]*') THEN 4 WHEN SCHEMA_NAME LIKE '%STG%' THEN 5 WHEN SCHEMA_NAME LIKE '%INT%' THEN 6 WHEN SCHEMA_NAME LIKE '%PRS%' THEN 7 ELSE 10 end ORd FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.SCHEMATA ORDER BY 2,1;"
    // Run the statement.
    var stmt = snowflake.createStatement({
        sqlText: sql_command,
        binds: [SOURCE_DB] //submit the source database
    });
    var res = stmt.execute();
    while (res.next()) {
        var schema = res.getColumnValue(1);
        schema_names.push(schema);
    }

} else {
    schema_names.push(SCHEMA_FILTER.trim().toUpperCase());
}

// Read each schema and execute copy schema by schema
var schema_num = 1;

schema_names.forEach(function (schema_name) {

    //set execution environment to SOURCE_DB to read all the ddls 
    snowflake.execute({
        sqlText: 'use database ' + SOURCE_DB + ';'
    });
    snowflake.execute({
        sqlText: 'use schema ' + schema_name + ';'
    });
    // OBJECT_TYPE is used to determine whether we want to move a specific or all object types 
    var obj_type = (OBJECT_TYPE === null || OBJECT_TYPE.trim() === '' || OBJECT_TYPE.trim().toUpperCase() === 'ALL') ?
        'ALL' :
        OBJECT_TYPE.trim().toUpperCase();
    // For each object there are different copy procedures- either clone or redeploy... 
    switch (obj_type) {
        case "ALL":
            //Execute for Tables
            //create a list of execute statements utilizing zero copy clone
            // TODO 1 : Like is slow | Use Contain | prefix
            var table_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " AND TABLE_NAME LIKE '" + OBJ_PATTERN + "%'";
            // Clone Tables

            if (!MOVE_TABLE_STRUCTURE_ONLY) {
                sql_command = FIRST_RUN ?
                    "select 'CREATE ' || SPLIT_part(TABLE_TYPE,' ',-1) || ' IF NOT EXISTS '||?||  '.'||table_schema||'.\"'|| table_name ||'\" clone '||table_catalog||'.'||table_schema||'.\"'||table_name||'\";' clone_statement, IS_TRANSIENT FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE  TABLE_SCHEMA =?  AND  Table_type = 'BASE TABLE'" + table_filter + " ORDER BY 1 " :
                    "select 'CREATE or REPLACE ' || SPLIT_part(TABLE_TYPE,' ',-1) || ' '||?||  '.'||table_schema||'.\"'|| table_name ||'\" clone '||table_catalog||'.'||table_schema||'.\"'||table_name||'\";' clone_statement, IS_TRANSIENT FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE  TABLE_SCHEMA =?  AND  Table_type = 'BASE TABLE'" + table_filter + " ORDER BY 1";

                var stmt = snowflake.createStatement({
                    sqlText: sql_command,
                    binds: [TARGET_DB, schema_name]
                });
                var res2 = stmt.execute();
                // Iterate through tables and add them to the list of commands to execute 
                while (res2.next()) {
                    row_as_json = res2.getColumnValue(1);
                    transient_val = res2.getColumnValue(2);
                    row_as_json = transient_val === 'NO'? row_as_json: row_as_json.replace(' TABLE ',' TRANSIENT TABLE '); 
                    // Add the row to the array of ddls.
                    if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                        array_of_rows.push(row_as_json);
                        ++table_num;
                    }
                }
            } else {
                // Move table structure only
                if (PRESERVE_TARGET_TABLE_DATA) {
                    //Get all source table names (non transient)
                    sql_command = "SELECT DISTINCT TABLE_NAME,IS_TRANSIENT from " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + schema_name + "' AND  TABLE_TYPE = 'BASE TABLE' " + table_filter + " ORDER BY 1";
                    var res2 = snowflake.execute({
                        sqlText: sql_command
                    });
                    //Iterate through each src table and compare structure
                    while (res2.next()) {
                        var table = res2.getColumnValue(1);
                        //Source table ddl
                        sql_command = "select get_ddl('table', '" + SOURCE_DB + "." + schema_name + "." + table + "');";
                        var resSrcDDL = snowflake.execute({
                            sqlText: sql_command
                        });
                        resSrcDDL.next();
                        var src_table_ddl = resSrcDDL.getColumnValue(1);
                        sql_command = "select get_ddl('table', '" + TARGET_DB + "." + schema_name + "." + table + "');";
                        try {
                            var resTgtDDL = snowflake.execute({
                                sqlText: sql_command
                            });
                            resTgtDDL.next();
                            var tgt_table_ddl = resTgtDDL.getColumnValue(1);
                        } catch (err) {
                            tgt_table_ddl = null;
                        }
                        // Src table exists in the target env but with different structure (ddl)
                        //   if ((tgt_table_ddl !== null) && (tgt_table_ddl !== '') && !FIRST_RUN && (src_table_ddl.replace(SOURCE_DB, TARGET_DB).toUpperCase().trim() !== tgt_table_ddl.toUpperCase().trim())) {
                        if ((tgt_table_ddl !== null) && (tgt_table_ddl !== '') && !FIRST_RUN) {
                            //Table strucute is different in SRC and TGT
                            //Check if table is non Empty
                            sql_command = `SELECT COUNT(*)
                                            FROM ` + TARGET_DB + `.INFORMATION_SCHEMA.TABLES
                                            WHERE Table_type = 'BASE TABLE'
                                                AND TABLE_SCHEMA = '` + schema_name + `'
                                                AND ROW_COUNT <> 0
                                                AND TABLE_NAME = '` + table + `'`;
                            var resCount = snowflake.execute({
                                sqlText: sql_command
                            });
                            resCount.next();
                            var count = resCount.getColumnValue(1);                           

                            if (count !== null && count !== '' && count !== 0) {
                                // Step 1: If count != 0 :- Take Backup with a different name (suffix - '_bkp')
                                sql_command = "select 'CREATE or REPLACE ' || SPLIT_part(TABLE_TYPE,' ',-1) || ' '||?||  '.'||table_schema||'.\"'|| table_name || '_bkp' ||'\" clone '||table_catalog||'.'||table_schema||'.\"'||table_name||'\";' clone_statement, IS_TRANSIENT FROM " + TARGET_DB + ".INFORMATION_SCHEMA.TABLES WHERE  TABLE_SCHEMA =?  AND  Table_type = 'BASE TABLE'  AND TABLE_NAME = '" + table + "' ORDER BY 1 ";
                                var stmt = snowflake.createStatement({
                                    sqlText: sql_command,
                                    binds: [TARGET_DB, schema_name]
                                });
                                var res3 = stmt.execute();
                                while (res3.next()) {
                                    row_as_json = res3.getColumnValue(1);
                                    transient_val = res3.getColumnValue(2);
                                    row_as_json = transient_val === 'NO'? row_as_json: row_as_json.replace(' TABLE ',' TRANSIENT TABLE '); 
                                    // Add the ddl to the array of ddls.
                                    if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                                        array_of_rows.push(row_as_json);
                                    }
                                }

                                // Step 2: Clone Src table to tgt
                                sql_command = "select 'CREATE or REPLACE ' || SPLIT_part(TABLE_TYPE,' ',-1) || ' '||?||  '.'||table_schema||'.\"'|| table_name ||'\" clone '||table_catalog||'.'||table_schema||'.\"'||table_name||'\";' clone_statement, IS_TRANSIENT FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA =?  AND  Table_type = 'BASE TABLE'  AND TABLE_NAME = '" + table + "' ORDER BY 1 ";
                                var stmt = snowflake.createStatement({
                                    sqlText: sql_command,
                                    binds: [TARGET_DB, schema_name]
                                });
                                var res3 = stmt.execute();
                                while (res3.next()) {
                                    row_as_json = res3.getColumnValue(1);
                                    transient_val = res3.getColumnValue(2);
                                    row_as_json = transient_val === 'NO'? row_as_json: row_as_json.replace(' TABLE ',' TRANSIENT TABLE '); 
                                    // Add the ddl to the array of ddls.
                                    if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                                        array_of_rows.push(row_as_json);
                                        ++table_num;
                                    }
                                }
                                // Step 3: Truncate the table just cloned to only keep the structure
                                array_of_rows.push("truncate table if exists " + TARGET_DB + "." + schema_name + ".\"" + table + "\";");
                            } else {
                                // If count == 0 : No Data, just clone the strucutre to tgt and truncate
                                sql_command = "select 'CREATE or REPLACE ' || SPLIT_part(TABLE_TYPE,' ',-1) || ' '||?||  '.'||table_schema||'.\"'|| table_name ||'\" clone '||table_catalog||'.'||table_schema||'.\"'||table_name||'\";' clone_statement, IS_TRANSIENT FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA =?  AND  Table_type = 'BASE TABLE'  AND TABLE_NAME = '" + table + "' ORDER BY 1 ";
                                var stmt = snowflake.createStatement({
                                    sqlText: sql_command,
                                    binds: [TARGET_DB, schema_name]
                                });
                                var res3 = stmt.execute();
                                while (res3.next()) {
                                    row_as_json = res3.getColumnValue(1);
                                    transient_val = res3.getColumnValue(2);
                                    row_as_json = transient_val === 'NO'? row_as_json: row_as_json.replace(' TABLE ',' TRANSIENT TABLE '); 
                                    // Add the ddl to the array of ddls.
                                    if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                                        array_of_rows.push(row_as_json);
                                        ++table_num;
                                    }
                                }
                                array_of_rows.push("truncate table if exists " + TARGET_DB + "." + schema_name + ".\"" + table + "\";");
                            }
                        }
                        // Src table dne in the target
                        else if (tgt_table_ddl === null || tgt_table_ddl === '') {
                            sql_command = FIRST_RUN ?
                                "select 'CREATE ' || SPLIT_part(TABLE_TYPE,' ',-1) || ' IF NOT EXISTS '||?||  '.'||table_schema||'.\"'|| table_name ||'\" clone '||table_catalog||'.'||table_schema||'.\"'||table_name||'\";' clone_statement, IS_TRANSIENT FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE  TABLE_SCHEMA =?  AND  Table_type = 'BASE TABLE'  AND TABLE_NAME = '" + table + "' ORDER BY 1 " :
                                "select 'CREATE or REPLACE ' || SPLIT_part(TABLE_TYPE,' ',-1) || ' '||?||  '.'||table_schema||'.\"'|| table_name ||'\" clone '||table_catalog||'.'||table_schema||'.\"'||table_name||'\";' clone_statement, IS_TRANSIENT FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE  TABLE_SCHEMA =?  AND  Table_type = 'BASE TABLE'  AND TABLE_NAME = '" + table + "' ORDER BY 1";
                            var stmt = snowflake.createStatement({
                                sqlText: sql_command,
                                binds: [TARGET_DB, schema_name]
                            });
                            var res3 = stmt.execute();
                            while (res3.next()) {
                                row_as_json = res3.getColumnValue(1);
                                transient_val = res3.getColumnValue(2);
                                row_as_json = transient_val === 'NO'? row_as_json: row_as_json.replace(' TABLE ',' TRANSIENT TABLE '); 
                                // Add the ddl to the array of ddls.
                                if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                                    array_of_rows.push(row_as_json);
                                    ++table_num;
                                }
                            }
                            array_of_rows.push("truncate table if exists " + TARGET_DB + "." + schema_name + ".\"" + table + "\";");
                        }
                    }
                } else {
                    //Get all source table names (non transient)
                    //sql_command = "SELECT DISTINCT TABLE_NAME from " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + schema_name + "' AND  TABLE_TYPE = 'BASE TABLE' AND IS_TRANSIENT = 'NO'" + table_filter + " ORDER BY 1";
                    sql_command = "SELECT DISTINCT TABLE_NAME, IS_TRANSIENT from " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + schema_name + "' AND  TABLE_TYPE = 'BASE TABLE' " + table_filter + " ORDER BY 1";
                    var res2 = snowflake.execute({
                        sqlText: sql_command
                    });
                    //Iterate through each src table
                    while (res2.next()) {
                        var table = res2.getColumnValue(1);
                        var istransient = res2.getColumnValue(2);
                        if(istransient === 'NO')
                        {
                            sql_command = FIRST_RUN ?
                                "select 'CREATE ' || SPLIT_part(TABLE_TYPE,' ',-1) || ' IF NOT EXISTS '||?||  '.'||table_schema||'.\"'|| table_name ||'\" clone '||table_catalog||'.'||table_schema||'.\"'||table_name||'\";' clone_statement FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE  TABLE_SCHEMA =?  AND  Table_type = 'BASE TABLE' AND IS_TRANSIENT = 'NO' AND TABLE_NAME = '" + table + "' ORDER BY 1" :
                                "select 'CREATE or REPLACE ' || SPLIT_part(TABLE_TYPE,' ',-1) || ' '||?||  '.'||table_schema||'.\"'|| table_name ||'\" clone '||table_catalog||'.'||table_schema||'.\"'||table_name||'\";' clone_statement FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE  TABLE_SCHEMA =?  AND  Table_type = 'BASE TABLE' AND IS_TRANSIENT = 'NO' AND TABLE_NAME = '" + table + "' ORDER BY 1 ";
                        }
                        else
                        {
                            sql_command = FIRST_RUN ?
                                "select 'CREATE TRANSIENT' || SPLIT_part(TABLE_TYPE,' ',-1) || ' IF NOT EXISTS '||?||  '.'||table_schema||'.\"'|| table_name ||'\" clone '||table_catalog||'.'||table_schema||'.\"'||table_name||'\";' clone_statement FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE  TABLE_SCHEMA =?  AND  Table_type = 'BASE TABLE'  AND TABLE_NAME = '" + table + "' ORDER BY 1" :
                                "select 'CREATE or REPLACE  TRANSIENT ' || SPLIT_part(TABLE_TYPE,' ',-1) || ' '||?||  '.'||table_schema||'.\"'|| table_name ||'\" clone '||table_catalog||'.'||table_schema||'.\"'||table_name||'\";' clone_statement FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE  TABLE_SCHEMA =?  AND  Table_type = 'BASE TABLE'  AND TABLE_NAME = '" + table + "' ORDER BY 1 ";
                        }
                           var stmt = snowflake.createStatement({
                                sqlText: sql_command,
                                binds: [TARGET_DB, schema_name]
                            });
                            var res3 = stmt.execute();
                            // Iterate through tables and add them to the list of commands to execute 
                            while (res3.next()) {
                                row_as_json = res3.getColumnValue(1);
                                // Add the row to the array of ddls.
                                if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                                    array_of_rows.push(row_as_json);
                                    ++table_num;
                                }
                            }
                            array_of_rows.push("truncate table if exists " + TARGET_DB + "." + schema_name + ".\"" + table + "\";");
                        }                        
                    }
                }
            

            //Execute for Views
            var view_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " AND TABLE_NAME LIKE '" + OBJ_PATTERN + "%'";
            if (FIRST_RUN) {
                sql_command = "select REPLACE(REPLACE(VIEW_DEFINITION, 'CREATE OR REPLACE VIEW', 'CREATE VIEW IF NOT EXISTS'), ?,?)  clone_statement, TABLE_NAME FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA =? AND VIEW_DEFINITION IS NOT NULL" + view_filter + " ORDER BY 1";
                var stmt = snowflake.createStatement({
                    sqlText: sql_command,
                    binds: [SOURCE_DB, TARGET_DB, schema_name]
                });
                var res2 = stmt.execute();
                while (res2.next()) {
                    row_as_json = res2.getColumnValue(1);
                    view = res2.getColumnValue(2); 
                    var str = row_as_json.indexOf(schema_name + "." + view) > 1 ? row_as_json : row_as_json.replace(view, schema_name + "." + view);
                    // Add the row to the array of ddls.
                    if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                        array_of_rows.push(str);
                        ++view_num;
                    }
                }
            } else {
                sql_command = "SELECT TABLE_NAME, VIEW_DEFINITION from " + SOURCE_DB + ".INFORMATION_SCHEMA.VIEWS where VIEW_DEFINITION IS NOT NULL AND TABLE_SCHEMA = '" + schema_name + "'" + view_filter + " ORDER BY 1";
                var res2 = snowflake.execute({
                    sqlText: sql_command
                });
                while (res2.next()) {
                    var view = res2.getColumnValue(1);
                    row_as_json = res2.getColumnValue(2);
                    var str = row_as_json.indexOf(schema_name + "." + view) > 1 ? row_as_json : row_as_json.replace(view, schema_name + "." + view);
                    // Add the row to the array of ddls.
                    if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                        array_of_rows.push(str);
                        ++view_num;
                    }                    
                }
            }

            //Execute for FILE_FORMATS
            //File format is a logical object, therefore cannot be cloned. can only be redeployed 
            var file_format_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " AND FILE_FORMAT_NAME LIKE '" + OBJ_PATTERN + "%'";
            sql_command = "select FILE_FORMAT_name || ''   FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.FILE_FORMATS WHERE FILE_FORMAT_SCHEMA =  ?" + file_format_filter + " ORDER BY 1";

            var stmt = snowflake.createStatement({
                sqlText: sql_command,
                binds: [schema_name]
            });
            var res2 = stmt.execute();
            //Iterate throuth all the file formats 
            while (res2.next()) {
                var file_form = res2.getColumnValue(1);
                var view = res2.getColumnValue(1);
				var schema_file_format = schema_name + "." + file_form;
                var fullyQualifiedSrcFFName = SOURCE_DB + "." + schema_name + "." + file_form;
                var replaceStr1 = 'CREATE OR REPLACE FILE FORMAT ' + file_form;
                var replaceStr2 = 'CREATE FILE FORMAT IF NOT EXISTS ' + schema_name + "." + file_form;
                var replaceStr3 = 'create or replace file format ' + file_form;
                var replaceStr4 = 'create file format if not exists ' + schema_name + "." + file_form;
                // Extract a format definition
                if (!FIRST_RUN) {
                    sql_command = "SELECT REPLACE(DDL,?,?) FROM (SELECT GET_DDL('file_format',?) AS DDL)";
                    var stmt = snowflake.createStatement({
                        sqlText: sql_command,
                        binds: [file_form, schema_file_format, file_form]
                    });
                } else {
                    sql_command = "select REPLACE(REPLACE(REPLACE(DDL, ?, ?), ?, ?), ?, ?) from (select get_ddl('file_format',?) AS DDL)";
                    var stmt = snowflake.createStatement({
                        sqlText: sql_command,
                        binds: [replaceStr1, replaceStr2, replaceStr3, replaceStr4, SOURCE_DB, TARGET_DB, fullyQualifiedSrcFFName]
                    });
                }
                var resf = stmt.execute();
                resf.next();
                // Modify in case of full qualified names
                row_as_json = resf.getColumnValue(1);
                row_as_json.replace(SOURCE_DB, TARGET_DB);

                // Add the row to the array of commands.
                if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                    array_of_rows.push(row_as_json);
                    ++ff_num;
                }
            }


            //Execute for STAGES
            var stage_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " AND STAGE_NAME LIKE '" + OBJ_PATTERN + "%'";
            sql_command = FIRST_RUN ?
                "select 'CREATE stage IF NOT EXISTS '  || ? || '.' ||STAGE_schema|| '.' || STAGE_name ||' clone '||STAGE_catalog||'.'||STAGE_schema||'.'||STAGE_name||';' clone_statement FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.STAGES WHERE STAGE_SCHEMA = ?" + stage_filter + " ORDER BY 1" :
                "select 'CREATE or REPLACE stage '  || ? || '.' ||STAGE_schema|| '.' || STAGE_name ||' clone '||STAGE_catalog||'.'||STAGE_schema||'.'||STAGE_name||';' clone_statement FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.STAGES WHERE STAGE_SCHEMA = ?" + stage_filter + " ORDER BY 1";
            var stmt = snowflake.createStatement({
                sqlText: sql_command,
                binds: [TARGET_DB, schema_name]
            });
            var res2 = stmt.execute();
            while (res2.next()) {
                row_as_json = res2.getColumnValue(1);
                // Add the row to the array of ddls.
                if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                    array_of_rows.push(row_as_json);
                    ++stage_num;
                }
            }

            //Execute for PROCEDURES
			//procedure statement is redesigned to fit the target database based on the metadata stored in procedures repository.
			var procedure_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " LIKE '" + OBJ_PATTERN + "%'";
            snowflake.execute({
                sqlText: 'show procedures' + procedure_filter + ' IN ' + SOURCE_DB + '.' + schema_name + ';'
            });
            sql_command = "SELECT \"name\", SPLIT_PART(\"arguments\", ' RETURN', 0 ) from table(result_scan(last_query_id())) ORDER BY 1;";
            var stmt = snowflake.createStatement({
                sqlText: sql_command
            });
            var res2 = stmt.execute();
            while (res2.next()) {
                var proc_name = res2.getColumnValue(1);				
			    var arg_name = res2.getColumnValue(2);						
			    des_command = "DESCRIBE PROCEDURE " + arg_name + ";";
				var stmt_describe = snowflake.createStatement({ sqlText: des_command });
				var res3 = stmt_describe.execute();
				
				while (res3.next()) {
					if (res3.getColumnValue(1) === "returns") { var returns_text = res3.getColumnValue(2) }
					if (res3.getColumnValue(1) === "language") { var language_text = res3.getColumnValue(2) }
					if (res3.getColumnValue(1) === "execute as") { var execute_as_text = res3.getColumnValue(2) }
					if (res3.getColumnValue(1) === "signature") { var signature_text = res3.getColumnValue(2) }
					if (res3.getColumnValue(1) === "body") { var body_text = res3.getColumnValue(2) }
							}

                if (execute_as_text === "OWNER") {				
                  owner_command = "SELECT 1 FROM EDW_OPS.INT.OWNER_RIGHT_PROCS WHERE PROCEDURE_NAME = '" + arg_name + "' AND PROCEDURE_SCHEMA = '" + schema_name + "';"; 
				  var stmt_owner = snowflake.createStatement({ sqlText: owner_command });
				  var res4 = stmt_owner.execute();
				  execute_as_text = "CALLER";
				  res_message = "Procedure " + arg_name + " migrated with 'EXECUTE AS CALLER', where as in source database created with 'OWNER'";
				
				  while (res4.next()) {					
				    if (res4.getColumnValue(1) === 1) {execute_as_text = "OWNER"; res_message = ""; }
				            }
				       } 

			    var str_ddl = FIRST_RUN ?
                   "CREATE PROCEDURE IF NOT EXISTS " + TARGET_DB + "." + schema_name + "." + proc_name + signature_text + " RETURNS " + returns_text + " LANGUAGE " + language_text + " EXECUTE AS " + execute_as_text + " AS " + "\$\$ " + body_text + " \$\$;":
				   "CREATE OR REPLACE PROCEDURE " + TARGET_DB + "." + schema_name + "." + proc_name + signature_text + " RETURNS " + returns_text + " LANGUAGE " + language_text + " EXECUTE AS " + execute_as_text + " AS " + "\$\$ " + body_text + " \$\$;";
				   
				if (str_ddl !== null && str_ddl.replace(/ /g, '') !== '') {
                    array_of_rows.push(str_ddl);
                    ++sp_num;
                  }
				}

            //Execute for FUNCTIONS
            var function_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " AND FUNCTION_NAME LIKE '" + OBJ_PATTERN + "%'";
            sql_command = FIRST_RUN ?
                "select 'CREATE FUNCTION IF NOT EXISTS '  || ? ||'.'||FUNCTION_schema||'.'||FUNCTION_name  ||  ARGUMENT_SIGNATURE || ' RETURNS ' || DATA_TYPE || ' LANGUAGE JAVASCRIPT AS \$\$ ' || FUNCTION_DEFINITION || ' \$\$;' clone_statement  FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.FUNCTIONS WHERE FUNCTION_SCHEMA = ?" + function_filter + " ORDER BY 1" :
                "select 'CREATE or REPLACE FUNCTION '  || ? ||'.'||FUNCTION_schema||'.'||FUNCTION_name  ||  ARGUMENT_SIGNATURE || ' RETURNS ' || DATA_TYPE || ' LANGUAGE JAVASCRIPT AS \$\$ ' || FUNCTION_DEFINITION || ' \$\$;' clone_statement  FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.FUNCTIONS WHERE FUNCTION_SCHEMA = ?" + function_filter + " ORDER BY 1";
            var stmt = snowflake.createStatement({
                sqlText: sql_command,
                binds: [TARGET_DB, schema_name]
            });
            var res2 = stmt.execute();
            while (res2.next()) {
                row_as_json = res2.getColumnValue(1);
                // Add the row to the array of ddls.
                if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                    array_of_rows.push(row_as_json);
                    ++fun_num;
                }
            }
			
			//Execute for SEQUENCE
            var sequence_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " AND SEQUENCE_NAME LIKE '" + OBJ_PATTERN + "%'";
            sql_command = FIRST_RUN ?
                "SELECT 'CREATE SEQUENCE IF NOT EXISTS ' || ?  ||'.'|| SEQUENCE_SCHEMA || '.' || SEQUENCE_NAME || ' START WITH ' || START_VALUE || ' INCREMENT BY ' || \"INCREMENT\" || ';' FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.SEQUENCES WHERE SEQUENCE_SCHEMA = ?" + sequence_filter + " ORDER BY 1" :
                "SELECT 'CREATE OR REPLACE SEQUENCE ' || ?  ||'.'|| SEQUENCE_SCHEMA || '.' ||SEQUENCE_NAME || ' START WITH ' || START_VALUE || ' INCREMENT BY ' || \"INCREMENT\" || ';' FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.SEQUENCES WHERE SEQUENCE_SCHEMA = ?" + sequence_filter + " ORDER BY 1";
            var stmt = snowflake.createStatement({
                sqlText: sql_command,
                binds: [TARGET_DB, schema_name]
            });
            var res2 = stmt.execute();
            while (res2.next()) {
                row_as_json = res2.getColumnValue(1);
                // Add the row to the array of ddls.
                if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                    array_of_rows.push(row_as_json);
                    ++seq_num;
                }
            }

            var streams_tasks_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " LIKE '" + OBJ_PATTERN + "%'";

            //Execute for stream
            snowflake.execute({
                sqlText: 'show streams' + streams_tasks_filter + ' IN ' + SOURCE_DB + '.' + schema_name + ';'
            });
            sql_command = "select * from table(result_scan(last_query_id())) order by \"name\";";
            var stmt = snowflake.createStatement({
                sqlText: sql_command
            });
            var res2 = stmt.execute();
            while (res2.next()) {
                var sliced_tblname = "";
                var name = res2.getColumnValue(2);
                var table_name = res2.getColumnValue(7);
                if (table_name === null || table_name === '')
                    continue;
                //if table_name is not fully qualified name, then create one
                //if table_name ends with . then skip this stream
                //if table_name is fully qualified then ensure it points to target database 
                if (table_name.lastIndexOf(".") == -1)
                    sliced_tblname = table_name;
                else if (table_name.length == table_name.lastIndexOf(".") + 1)
                    continue;
                else
                    sliced_tblname = table_name.substring(table_name.lastIndexOf(".") + 1);
                var src_tblname = TARGET_DB + "." + schema_name + "." + sliced_tblname;
                table_txt = table_name.replace(SOURCE_DB, TARGET_DB) === src_tblname ? " ON TABLE " + src_tblname : " ON TABLE " + table_name.replace(SOURCE_DB, TARGET_DB);
                var mode_name = res2.getColumnValue(10);
                var mode_txt = mode_name === null || mode_name === '' || mode_name.toUpperCase() === 'DEFAULT' ? '' : " " + mode_name + " = TRUE;"
                var str_ddl = FIRST_RUN ?
                    "CREATE STREAM IF NOT EXISTS " + TARGET_DB + "." + schema_name + "." + name + table_txt + mode_txt :
                    "CREATE OR REPLACE STREAM " + TARGET_DB + "." + schema_name + "." + name + table_txt + mode_txt;
                //Add the stream definition to the array of commands
                if (str_ddl !== null && str_ddl.replace(/ /g, '') !== '') {
                    array_of_rows.push(str_ddl);
                    ++stream_num;
                }

            }


            //Execute for task 
            snowflake.execute({
                sqlText: 'show tasks' + streams_tasks_filter + ' IN ' + SOURCE_DB + '.' + schema_name + ';'
            });
            sql_command = `WITH recursive cte
                                            (
                                            created_on
                                            ,name
                                            ,id
                                            ,database_name
                                            ,schema_name
                                            ,OWNER
                                            ,comment
                                            ,warehouse
                                            ,schedule
                                            ,predecessors
                                            ,STATE
                                            ,DEFINITION
                                            ,condition
											,allow_overlapping_execution
                                            ,LEVEL
                                            )
                                        AS
                                            (
                                            SELECT \"created_on\"
                                                ,\"name\"
                                                ,\"id\"
                                                ,\"database_name\"
                                                ,\"schema_name\"
                                                ,\"owner\"
                                                ,\"comment\"
                                                ,\"warehouse\"
                                                ,\"schedule\"
                                                ,SPLIT(\"predecessors\", '.') [2] AS \"predecessors\"
                                                ,\"state\"
                                                ,\"definition\"
                                                ,\"condition\"
												,\"allow_overlapping_execution\"
                                                ,0 AS \"level\"
                                            FROM TABLE (result_scan(last_query_id()))
                                            WHERE NVL(SPLIT(\"predecessors\", '.') [2], 'NULL')  != all (select NVL(e.\"name\",'NULL') from TABLE (result_scan(last_query_id())) e)
                                            
                                            UNION ALL
                                            
                                            SELECT A.\"created_on\"
                                                ,A.\"name\"
                                                ,A.\"id\"
                                                ,A.\"database_name\"
                                                ,A.\"schema_name\"
                                                ,A.\"owner\"
                                                ,A.\"comment\"
                                                ,A.\"warehouse\"
                                                ,A.\"schedule\"
                                                ,SPLIT(A.\"predecessors\", '.') [2] AS \"predecessors\"
                                                ,A.\"state\"
                                                ,A.\"definition\"
                                                ,A.\"condition\"
												,A.\"allow_overlapping_execution\"
                                                ,B.LEVEL + 1
                                            FROM TABLE (result_scan(last_query_id())) AS A
                                            INNER JOIN cte AS B ON replace(SPLIT(A.\"predecessors\", '.') [2],'"','') = B.name
                                            )
                                        SELECT DISTINCT created_on
                                            ,name
                                            ,id
                                            ,database_name
                                            ,schema_name
                                            ,OWNER
                                            ,comment
                                            ,warehouse
                                            ,schedule
                                            ,predecessors
                                            ,STATE
                                            ,DEFINITION
                                            ,condition
											,allow_overlapping_execution
                                            ,LEVEL
                                        FROM cte
                                        ORDER BY LEVEL;`;
            var stmt = snowflake.createStatement({
                sqlText: sql_command
            });
            var res2 = stmt.execute();
            while (res2.next()) {
                var name = '"' + res2.getColumnValue(2) + '"';
				var comment = res2.getColumnValue(7);
                var wh = res2.getColumnValue(8);				
                var schedule = res2.getColumnValue(9);                			
                var after = res2.getColumnValue(10);
                var definations = res2.getColumnValue(12);
                var conditions = res2.getColumnValue(13);
				var overlapping_execution = res2.getColumnValue(14);
                var schedule_text = schedule === null || schedule === '' ? '' : " schedule='" + schedule + "'";				
				var comment_text = comment === null || comment === '' ? '' : " COMMENT='" + comment + "'";				
                var after_text = after === null || after === '' ? '' : " after  " + TARGET_DB + "." + schema_name + "." + after;
                var conditions_text = conditions === null || conditions === '' ? '' : " when   " + conditions + " ";
				var overlapping_execution_text = overlapping_execution === 'null' || overlapping_execution === '' ? '' : " ALLOW_OVERLAPPING_EXECUTION=" + overlapping_execution;
                
                default_wh = TARGET_DB + '_LVW'
                sql_command = "show warehouses like '" + TARGET_DB + "%';"
                var stmt = snowflake.createStatement({ sqlText: sql_command });
                var res = stmt.execute();
                
                while (res.next()) 
                    {
                        var warehouse = res.getColumnValue(1);                        
                        if (warehouse === TARGET_DB + wh.substring(wh.indexOf(SOURCE_DB) + SOURCE_DB.length,wh.length)) { var target_wh = 'YES'; }
                        else if( warehouse === default_wh ) { var def_wh = 'YES'; }
                    }
                
                if (TARGET_WH.trim().length > 0) { wh = TARGET_WH.trim().toUpperCase(); }    
                else if (target_wh === 'YES') { wh = TARGET_DB + wh.substring(wh.indexOf(SOURCE_DB) + SOURCE_DB.length,wh.length); }                 
                else if (def_wh === 'YES') { wh = default_wh; }
				
				// Handle Session Parameters for Task
                var session_parameter_text = "";				
				parm_command = "SHOW PARAMETERS IN TASK " + SOURCE_DB + '.' + schema_name + '.' + name + ";";
				var cmd = snowflake.createStatement({ sqlText: parm_command });		
				var res_set = cmd.execute();    	 	   
				   
				if (res_set.next()) { 		
				if (res_set.getColumnValue(4) === "TASK") {
				   if (res_set.getColumnValue(6) === "STRING")       
				      { session_parameter_text += res_set.getColumnValue(1) + "=" + "''" + res_set.getColumnValue(2) + "''" + " "} 
						   else { session_parameter_text += res_set.getColumnValue(1) + "=" + res_set.getColumnValue(2) + " " } 
					  }
					 }		   
						  
				while (res_set.next()) {
				 if (res_set.getColumnValue(4) === "TASK") {
				  if (res_set.getColumnValue(6) === "STRING")       
					{ session_parameter_text += res_set.getColumnValue(1) + "=" + "''" + res_set.getColumnValue(2) + "''" + " "} 
					  else { session_parameter_text += res_set.getColumnValue(1) + "=" + res_set.getColumnValue(2) + " "}       
					 	  }
				    }			

                if (session_parameter_text.trim().length > 0) { session_parameter_text = " " + session_parameter_text; }

                var str_ddl = FIRST_RUN ?
                    "CREATE TASK IF NOT EXISTS " + TARGET_DB + "." + schema_name + "." + name + " warehouse= " + wh + schedule_text + session_parameter_text + comment_text + overlapping_execution_text + after_text + conditions_text + " as " + definations + " ;" :
                    "CREATE OR REPLACE TASK  " + TARGET_DB + "." + schema_name + "." + name + " warehouse= " + wh + schedule_text + session_parameter_text + comment_text + overlapping_execution_text + after_text + conditions_text + " as " + definations + "  ;";
                //Add the task definition to the array of commands
                if (str_ddl !== null && str_ddl.replace(/ /g, '') !== '') {
                    array_of_rows.push(str_ddl);
                    ++task_num;
                }

            }
            break;

        case "TABLE":
            //Execute for Tables
            //create a list of execute statements utilizing zero copy clone
            // TODO 1 : Like is slow | Use Contain | prefix
            var table_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " AND TABLE_NAME LIKE '" + OBJ_PATTERN + "%'";
            // Clone Tables
            if (!MOVE_TABLE_STRUCTURE_ONLY) {
                sql_command = FIRST_RUN ?
                    "select 'CREATE ' || SPLIT_part(TABLE_TYPE,' ',-1) || ' IF NOT EXISTS '||?||  '.'||table_schema||'.\"'|| table_name ||'\" clone '||table_catalog||'.'||table_schema||'.\"'||table_name||'\";' clone_statement, IS_TRANSIENT FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE  TABLE_SCHEMA =?  AND  Table_type = 'BASE TABLE'" + table_filter + " ORDER BY 1" :
                    "select 'CREATE or REPLACE ' || SPLIT_part(TABLE_TYPE,' ',-1) || ' '||?||  '.'||table_schema||'.\"'|| table_name ||'\" clone '||table_catalog||'.'||table_schema||'.\"'||table_name||'\";' clone_statement, IS_TRANSIENT FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE  TABLE_SCHEMA =?  AND  Table_type = 'BASE TABLE'" + table_filter + " ORDER BY 1";
                var stmt = snowflake.createStatement({
                    sqlText: sql_command,
                    binds: [TARGET_DB, schema_name]
                });
                var res2 = stmt.execute();                
                // Iterate through tables and add them to the list of commands to execute 
                while (res2.next()) {
                    row_as_json = res2.getColumnValue(1);                    
                    transient_val = res2.getColumnValue(2);
                    row_as_json = transient_val === 'NO'? row_as_json: row_as_json.replace(' TABLE ',' TRANSIENT TABLE '); 
                    // Add the row to the array of ddls.                    
                    if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                        array_of_rows.push(row_as_json);
                        ++table_num;
                    }
                }
            } else {
                // Move table structure only
                if (PRESERVE_TARGET_TABLE_DATA) {
                    //Get all source table names (non transient)
                    //sql_command = "SELECT DISTINCT TABLE_NAME from " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + schema_name + "' AND  TABLE_TYPE = 'BASE TABLE' AND IS_TRANSIENT = 'NO'" + table_filter + " ORDER BY 1";
                    sql_command = "SELECT DISTINCT TABLE_NAME, IS_TRANSIENT from " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + schema_name + "' AND  TABLE_TYPE = 'BASE TABLE' " + table_filter + " ORDER BY 1";
                    var res2 = snowflake.execute({
                        sqlText: sql_command
                    });
                    //Iterate through each src table and compare structure
                    while (res2.next()) {
                        var table = res2.getColumnValue(1);
                        //Source table ddl
                        sql_command = "select get_ddl('table', '" + SOURCE_DB + "." + schema_name + "." + table + "');";
                        var resSrcDDL = snowflake.execute({
                            sqlText: sql_command
                        });
                        resSrcDDL.next();
                        var src_table_ddl = resSrcDDL.getColumnValue(1);
                        sql_command = "select get_ddl('table', '" + TARGET_DB + "." + schema_name + "." + table + "');";
                        try {
                            var resTgtDDL = snowflake.execute({
                                sqlText: sql_command
                            });
                            resTgtDDL.next();
                            var tgt_table_ddl = resTgtDDL.getColumnValue(1);
                        } catch (err) {
                            tgt_table_ddl = null;
                        }
                        // Src table exists in the target env but with different structure (ddl)
                        //if ((tgt_table_ddl !== null) && (tgt_table_ddl !== '') && !FIRST_RUN && (src_table_ddl.replace(SOURCE_DB, TARGET_DB).toUpperCase().trim() !== tgt_table_ddl.toUpperCase().trim()))
                        if ((tgt_table_ddl !== null) && (tgt_table_ddl !== '') && !FIRST_RUN) {
                            //Table strucute is different in SRC and TGT
                            //Check if table is non Empty
                            sql_command = `SELECT COUNT(*)
                                            FROM ` + TARGET_DB + `.INFORMATION_SCHEMA.TABLES
                                            WHERE Table_type = 'BASE TABLE'
                                                AND TABLE_SCHEMA = '` + schema_name + `'
                                                AND ROW_COUNT <> 0
                                                AND TABLE_NAME = '` + table + `'`;
                            var resCount = snowflake.execute({
                                sqlText: sql_command
                            });
                            resCount.next();
                            var count = resCount.getColumnValue(1);

                            // Get the count of target_bkp table if already exists 
                            sql_command2 = `SELECT COUNT(*)
                                            FROM ` + TARGET_DB + `.INFORMATION_SCHEMA.TABLES
                                            WHERE Table_type = 'BASE TABLE'
                                                AND TABLE_SCHEMA = '` + schema_name + `'
                                                AND ROW_COUNT <> 0
                                                AND TABLE_NAME = '` + table + `_bkp'`;
                            var resBkpCount = snowflake.execute({
                                sqlText: sql_command2
                            });
                            resBkpCount.next();
                            var bkpcount = resBkpCount.getColumnValue(1);

                            if (count !== null && count !== '' && count !== 0) {
                                // Step 1: If count != 0 :- Take Backup with a different name (suffix - '_bkp')
                                //sql_command = "select 'CREATE or REPLACE ' || SPLIT_part(TABLE_TYPE,' ',-1) || ' '||?||  '.'||table_schema||'.\"'|| table_name || '_bkp' ||'\" clone '||table_catalog||'.'||table_schema||'.\"'||table_name||'\";' clone_statement FROM " + TARGET_DB + ".INFORMATION_SCHEMA.TABLES WHERE  TABLE_SCHEMA =?  AND  Table_type = 'BASE TABLE' AND IS_TRANSIENT = 'NO' AND TABLE_NAME = '" + table + "' ORDER BY 1";
                                sql_command = "select 'CREATE or REPLACE ' || SPLIT_part(TABLE_TYPE,' ',-1) || ' '||?||  '.'||table_schema||'.\"'|| table_name || '_bkp' ||'\" clone '||table_catalog||'.'||table_schema||'.\"'||table_name||'\";' clone_statement, IS_TRANSIENT FROM " + TARGET_DB + ".INFORMATION_SCHEMA.TABLES WHERE  TABLE_SCHEMA =?  AND  Table_type = 'BASE TABLE'  AND TABLE_NAME = '" + table + "' ORDER BY 1";
                                var stmt = snowflake.createStatement({
                                    sqlText: sql_command,
                                    binds: [TARGET_DB, schema_name]
                                });
                                var res3 = stmt.execute();
                                while (res3.next()) {
                                    row_as_json = res3.getColumnValue(1);
                                    transient_val = res3.getColumnValue(2);
                                    row_as_json = transient_val === 'NO'? row_as_json: row_as_json.replace(' TABLE ',' TRANSIENT TABLE '); 
                                    // Add the ddl to the array of ddls.
                                    if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                                        array_of_rows.push(row_as_json);
                                    }
                                }

                                // Step 2: Clone Src table to tgt
                                sql_command = "select 'CREATE or REPLACE ' || SPLIT_part(TABLE_TYPE,' ',-1) || ' '||?||  '.'||table_schema||'.\"'|| table_name ||'\" clone '||table_catalog||'.'||table_schema||'.\"'||table_name||'\";' clone_statement, IS_TRANSIENT FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA =?  AND  Table_type = 'BASE TABLE' AND TABLE_NAME = '" + table + "' ORDER BY 1";
                                var stmt = snowflake.createStatement({
                                    sqlText: sql_command,
                                    binds: [TARGET_DB, schema_name]
                                });
                                var res3 = stmt.execute();
                                while (res3.next()) {
                                    row_as_json = res3.getColumnValue(1);
                                    transient_val = res3.getColumnValue(2);
                                    row_as_json = transient_val === 'NO'? row_as_json: row_as_json.replace(' TABLE ',' TRANSIENT TABLE '); 
                                    // Add the ddl to the array of ddls.
                                    if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                                        array_of_rows.push(row_as_json);
                                        ++table_num;
                                    }
                                }
                                // Step 3: Truncate the table just cloned to only keep the structure
                                array_of_rows.push("truncate table if exists " + TARGET_DB + "." + schema_name + ".\"" + table + "\";");
                            } else {
                                // If count == 0 : No Data, just clone the strucutre to tgt and truncate
                                sql_command = "select 'CREATE or REPLACE ' || SPLIT_part(TABLE_TYPE,' ',-1) || ' '||?||  '.'||table_schema||'.\"'|| table_name ||'\" clone '||table_catalog||'.'||table_schema||'.\"'||table_name||'\";' clone_statement, IS_TRANSIENT FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA =?  AND  Table_type = 'BASE TABLE'  AND TABLE_NAME = '" + table + "' ORDER BY 1";
                                var stmt = snowflake.createStatement({
                                    sqlText: sql_command,
                                    binds: [TARGET_DB, schema_name]
                                });
                                var res3 = stmt.execute(); 
                                while (res3.next()) {
                                    row_as_json = res3.getColumnValue(1);
                                    transient_val = res3.getColumnValue(2);
                                    row_as_json = transient_val === 'NO'? row_as_json: row_as_json.replace(' TABLE ',' TRANSIENT TABLE '); 
                                    // Add the ddl to the array of ddls.
                                    if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                                        array_of_rows.push(row_as_json);
                                        ++table_num;
                                    }
                                }
                                array_of_rows.push("truncate table if exists " + TARGET_DB + "." + schema_name + ".\"" + table + "\";");
                            }
                        }
                        // Src table dne in the target
                        else if (tgt_table_ddl === null || tgt_table_ddl === '') {
                            sql_command = FIRST_RUN ?
                                "select 'CREATE ' || SPLIT_part(TABLE_TYPE,' ',-1) || ' IF NOT EXISTS '||?||  '.'||table_schema||'.\"'|| table_name ||'\" clone '||table_catalog||'.'||table_schema||'.\"'||table_name||'\";' clone_statement, IS_TRANSIENT FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE  TABLE_SCHEMA =?  AND  Table_type = 'BASE TABLE'  AND TABLE_NAME = '" + table + "' ORDER BY 1" :
                                "select 'CREATE or REPLACE ' || SPLIT_part(TABLE_TYPE,' ',-1) || ' '||?||  '.'||table_schema||'.\"'|| table_name ||'\" clone '||table_catalog||'.'||table_schema||'.\"'||table_name||'\";' clone_statement, IS_TRANSIENT FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE  TABLE_SCHEMA =?  AND  Table_type = 'BASE TABLE' AND TABLE_NAME = '" + table + "' ORDER BY 1";
                            var stmt = snowflake.createStatement({
                                sqlText: sql_command,
                                binds: [TARGET_DB, schema_name]
                            });
                            var res3 = stmt.execute();
                            while (res3.next()) {
                                row_as_json = res3.getColumnValue(1);
                                transient_val = res3.getColumnValue(2);
                                row_as_json = transient_val === 'NO'? row_as_json: row_as_json.replace(' TABLE ',' TRANSIENT TABLE '); 
                                // Add the ddl to the array of ddls.
                                if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                                    array_of_rows.push(row_as_json);
                                    ++table_num;
                                }
                            }                            
                            array_of_rows.push("truncate table if exists " + TARGET_DB + "." + schema_name + ".\"" + table + "\";");
                        }
                    }
                } else {
                    //Get all source table names 
                    sql_command = "SELECT DISTINCT TABLE_NAME , IS_TRANSIENT from " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + schema_name + "' AND  TABLE_TYPE = 'BASE TABLE' " + table_filter + " ORDER BY 1";
                    var res2 = snowflake.execute({
                        sqlText: sql_command
                    });
                    //Iterate through each src table
                    while (res2.next()) {
                        var table = res2.getColumnValue(1);
                        var istransient = res2.getColumnValue(2);
                        if (istransient === 'NO')
                        {
                            sql_command = FIRST_RUN ?
                                "select 'CREATE ' || SPLIT_part(TABLE_TYPE,' ',-1) || ' IF NOT EXISTS '||?||  '.'||table_schema||'.\"'|| table_name ||'\" clone '||table_catalog||'.'||table_schema||'.\"'||table_name||'\";' clone_statement FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE  TABLE_SCHEMA =?  AND  Table_type = 'BASE TABLE' AND IS_TRANSIENT = 'NO' AND TABLE_NAME = '" + table + "' ORDER BY 1" :
                                "select 'CREATE or REPLACE ' || SPLIT_part(TABLE_TYPE,' ',-1) || ' '||?||  '.'||table_schema||'.\"'|| table_name ||'\" clone '||table_catalog||'.'||table_schema||'.\"'||table_name||'\";' clone_statement FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE  TABLE_SCHEMA =?  AND  Table_type = 'BASE TABLE' AND IS_TRANSIENT = 'NO' AND TABLE_NAME = '" + table + "' ORDER BY 1";
                        }   
                        else 
                        {
                            sql_command = FIRST_RUN ?
                                "select 'CREATE TRANSIENT' || SPLIT_part(TABLE_TYPE,' ',-1) || ' IF NOT EXISTS '||?||  '.'||table_schema||'.\"'|| table_name ||'\" clone '||table_catalog||'.'||table_schema||'.\"'||table_name||'\";' clone_statement FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE  TABLE_SCHEMA =?  AND  Table_type = 'BASE TABLE' AND IS_TRANSIENT = 'NO' AND TABLE_NAME = '" + table + "' ORDER BY 1" :
                                "select 'CREATE or REPLACE TRANSIENT' || SPLIT_part(TABLE_TYPE,' ',-1) || ' '||?||  '.'||table_schema||'.\"'|| table_name ||'\" clone '||table_catalog||'.'||table_schema||'.\"'||table_name||'\";' clone_statement FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE  TABLE_SCHEMA =?  AND  Table_type = 'BASE TABLE' AND IS_TRANSIENT = 'NO' AND TABLE_NAME = '" + table + "' ORDER BY 1";
                        }
                        var stmt = snowflake.createStatement({
                                sqlText: sql_command,
                                binds: [TARGET_DB, schema_name]
                            });
                        var res3 = stmt.execute();
                            // Iterate through tables and add them to the list of commands to execute 
                        while (res3.next()) {
                            row_as_json = res3.getColumnValue(1);                                                
                            // Add the row to the array of ddls.
                            if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                                array_of_rows.push(row_as_json);
                                ++table_num;
                            }
                        }
                        array_of_rows.push("truncate table if exists " + TARGET_DB + "." + schema_name + ".\"" + table + "\";");                            
                    }
                }
            }
            break;

        case "VIEW":
            //Execute for Views
            var view_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " AND TABLE_NAME LIKE '" + OBJ_PATTERN + "%'";
            if (FIRST_RUN) {
                sql_command = "select REPLACE(REPLACE(VIEW_DEFINITION, 'CREATE OR REPLACE VIEW', 'CREATE VIEW IF NOT EXISTS'), ?,?)  clone_statement, TABLE_NAME FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA =? AND VIEW_DEFINITION IS NOT NULL" + view_filter + " ORDER BY 1";
                var stmt = snowflake.createStatement({
                    sqlText: sql_command,
                    binds: [SOURCE_DB, TARGET_DB, schema_name]
                });
                var res2 = stmt.execute();
                while (res2.next()) {
                    row_as_json = res2.getColumnValue(1);
                    view = res2.getColumnValue(2); 
                    var str = row_as_json.indexOf(schema_name + "." + view) > 1 ? row_as_json : row_as_json.replace(view, schema_name + "." + view);
                    // Add the row to the array of ddls.
                    if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                        array_of_rows.push(str);
                        ++view_num;
                    }
                }
            } else {
                sql_command = "SELECT TABLE_NAME, VIEW_DEFINITION from " + SOURCE_DB + ".INFORMATION_SCHEMA.VIEWS where VIEW_DEFINITION IS NOT NULL AND TABLE_SCHEMA = '" + schema_name + "'" + view_filter + " ORDER BY 1";
                var res2 = snowflake.execute({
                    sqlText: sql_command
                });
                while (res2.next()) {
                    var view = res2.getColumnValue(1);
                    row_as_json = res2.getColumnValue(2);
                    var str = row_as_json.indexOf(schema_name + "." + view) > 1 ? row_as_json : row_as_json.replace(view, schema_name + "." + view);
                    // Add the row to the array of ddls.
                    if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                        array_of_rows.push(str);
                        ++view_num;
                    }                   
                }
            }
            break;

        case "FF":
            //Execute for FILE_FORMATS
            //File format is a logical object, therefore cannot be cloned. can only be redeployed 
            var file_format_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " AND FILE_FORMAT_NAME LIKE '" + OBJ_PATTERN + "%'";
            sql_command = "select FILE_FORMAT_name || ''   FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.FILE_FORMATS WHERE FILE_FORMAT_SCHEMA =  ?" + file_format_filter + " ORDER BY 1";

            var stmt = snowflake.createStatement({
                sqlText: sql_command,
                binds: [schema_name]
            });
            var res2 = stmt.execute();
            //Iterate throuth all the file formats 
            while (res2.next()) {
                var file_form = res2.getColumnValue(1);
                var view = res2.getColumnValue(1);
				var schema_file_format = schema_name + "." + file_form;
                var fullyQualifiedSrcFFName = SOURCE_DB + "." + schema_name + "." + file_form;
                var replaceStr1 = 'CREATE OR REPLACE FILE FORMAT ' + file_form;
                var replaceStr2 = 'CREATE FILE FORMAT IF NOT EXISTS ' + schema_name + "." + file_form;
                var replaceStr3 = 'create or replace file format ' + file_form;
                var replaceStr4 = 'create file format if not exists ' + schema_name + "." + file_form;
                // Extract a format definition
                if (!FIRST_RUN) {
                    sql_command = "SELECT REPLACE(DDL,?,?) FROM (SELECT GET_DDL('file_format',?) AS DDL)";
                    var stmt = snowflake.createStatement({
                        sqlText: sql_command,
                        binds: [file_form, schema_file_format, file_form]
                    });
                } else {
                    sql_command = "select REPLACE(REPLACE(REPLACE(DDL, ?, ?), ?, ?), ?, ?) from (select get_ddl('file_format',?) AS DDL)";
                    var stmt = snowflake.createStatement({
                        sqlText: sql_command,
                        binds: [replaceStr1, replaceStr2, replaceStr3, replaceStr4, SOURCE_DB, TARGET_DB, fullyQualifiedSrcFFName]
                    });
                }
                var resf = stmt.execute();
                resf.next();
                // Modify in case of full qualified names
                row_as_json = resf.getColumnValue(1);
                row_as_json.replace(SOURCE_DB, TARGET_DB);

                // Add the row to the array of commands.
                if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                    array_of_rows.push(row_as_json);
                    ++ff_num;
                }
            }
            break;

        case "STAGES":
            //Execute for STAGES
            var stage_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " AND STAGE_NAME LIKE '" + OBJ_PATTERN + "%'";
            sql_command = FIRST_RUN ?
                "select 'CREATE stage IF NOT EXISTS '  || ? || '.' ||STAGE_schema|| '.' || STAGE_name ||' clone '||STAGE_catalog||'.'||STAGE_schema||'.'||STAGE_name||';' clone_statement FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.STAGES WHERE STAGE_SCHEMA = ?" + stage_filter + " ORDER BY 1" :
                "select 'CREATE or REPLACE stage '  || ? || '.' ||STAGE_schema|| '.' || STAGE_name ||' clone '||STAGE_catalog||'.'||STAGE_schema||'.'||STAGE_name||';' clone_statement FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.STAGES WHERE STAGE_SCHEMA = ?" + stage_filter + " ORDER BY 1";
            var stmt = snowflake.createStatement({
                sqlText: sql_command,
                binds: [TARGET_DB, schema_name]
            });
            var res2 = stmt.execute();
            while (res2.next()) {
                row_as_json = res2.getColumnValue(1);
                // Add the row to the array of ddls.
                if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                    array_of_rows.push(row_as_json);
                    ++stage_num;
                }
            }
            break;

        case "SP":
            //Execute for PROCEDURES
			//procedure statement is redesigned to fit the target database based on the metadata stored in procedures repository.
			var procedure_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " LIKE '" + OBJ_PATTERN + "%'";
            snowflake.execute({
                sqlText: 'show procedures' + procedure_filter + ' IN ' + SOURCE_DB + '.' + schema_name + ';'
            });
            sql_command = "SELECT \"name\", SPLIT_PART(\"arguments\", ' RETURN', 0 ) from table(result_scan(last_query_id())) ORDER BY 1;";
            var stmt = snowflake.createStatement({
                sqlText: sql_command
            });
            var res2 = stmt.execute();
            while (res2.next()) {
                var proc_name = res2.getColumnValue(1);				
			    var arg_name = res2.getColumnValue(2);						
			    des_command = "DESCRIBE PROCEDURE " + arg_name + ";";
				var stmt_describe = snowflake.createStatement({ sqlText: des_command });
				var res3 = stmt_describe.execute();
				
				while (res3.next()) {
					if (res3.getColumnValue(1) === "returns") { var returns_text = res3.getColumnValue(2) }
					if (res3.getColumnValue(1) === "language") { var language_text = res3.getColumnValue(2) }
					if (res3.getColumnValue(1) === "execute as") { var execute_as_text = res3.getColumnValue(2) }
					if (res3.getColumnValue(1) === "signature") { var signature_text = res3.getColumnValue(2) }
					if (res3.getColumnValue(1) === "body") { var body_text = res3.getColumnValue(2) }
							}

                 if (execute_as_text === "OWNER") {				
                   owner_command = "SELECT 1 FROM EDW_OPS.INT.OWNER_RIGHT_PROCS WHERE PROCEDURE_NAME = '" + arg_name + "' AND PROCEDURE_SCHEMA = '" + schema_name + "';"; 
				   var stmt_owner = snowflake.createStatement({ sqlText: owner_command });
				   var res4 = stmt_owner.execute();
				   execute_as_text = "CALLER";
				   res_message = "Procedure " + arg_name + " migrated with 'EXECUTE AS CALLER', where as in source database created with 'OWNER'";
								
				  while (res4.next()) {					
				    if (res4.getColumnValue(1) === 1) {execute_as_text = "OWNER"; res_message = ""; }
				            }
				       }

			    var str_ddl = FIRST_RUN ?
                   "CREATE PROCEDURE IF NOT EXISTS " + TARGET_DB + "." + schema_name + "." + proc_name + signature_text + " RETURNS " + returns_text + " LANGUAGE " + language_text + " EXECUTE AS " + execute_as_text + " AS " + "\$\$ " + body_text + " \$\$;":
				   "CREATE OR REPLACE PROCEDURE " + TARGET_DB + "." + schema_name + "." + proc_name + signature_text + " RETURNS " + returns_text + " LANGUAGE " + language_text + " EXECUTE AS " + execute_as_text + " AS " + "\$\$ " + body_text + " \$\$;";
				   
				if (str_ddl !== null && str_ddl.replace(/ /g, '') !== '') {
                    array_of_rows.push(str_ddl);
                    ++sp_num;
                  }
				}
            break;

        case "FUNCTION":
            //Execute for FUNCTIONS
            var function_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " AND FUNCTION_NAME LIKE '" + OBJ_PATTERN + "%'";
            sql_command = FIRST_RUN ?
                "select 'CREATE FUNCTION IF NOT EXISTS '  || ? ||'.'||FUNCTION_schema||'.'||FUNCTION_name  ||  ARGUMENT_SIGNATURE || ' RETURNS ' || DATA_TYPE || ' LANGUAGE JAVASCRIPT AS \$\$ ' || FUNCTION_DEFINITION || ' \$\$;' clone_statement  FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.FUNCTIONS WHERE FUNCTION_SCHEMA = ?" + function_filter + " ORDER BY 1" :
                "select 'CREATE or REPLACE FUNCTION '  || ? ||'.'||FUNCTION_schema||'.'||FUNCTION_name  ||  ARGUMENT_SIGNATURE || ' RETURNS ' || DATA_TYPE || ' LANGUAGE JAVASCRIPT AS \$\$ ' || FUNCTION_DEFINITION || ' \$\$;' clone_statement  FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.FUNCTIONS WHERE FUNCTION_SCHEMA = ?" + function_filter + " ORDER BY 1";
            var stmt = snowflake.createStatement({
                sqlText: sql_command,
                binds: [TARGET_DB, schema_name]
            });
            var res2 = stmt.execute();
            while (res2.next()) {
                row_as_json = res2.getColumnValue(1);
                // Add the row to the array of ddls.
                if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                    array_of_rows.push(row_as_json);
                    ++fun_num;
                }
            }
            break;
			
		case "SEQ":
            //Execute for SEQUENCE
            var sequence_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " AND SEQUENCE_NAME LIKE '" + OBJ_PATTERN + "%'";
            sql_command = FIRST_RUN ?
                "SELECT 'CREATE SEQUENCE IF NOT EXISTS ' || ?  ||'.'|| SEQUENCE_SCHEMA || '.' ||SEQUENCE_NAME || ' START WITH ' || START_VALUE || ' INCREMENT BY ' || \"INCREMENT\" || ';' FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.SEQUENCES WHERE SEQUENCE_SCHEMA = ?" + sequence_filter + " ORDER BY 1" :
                "SELECT 'CREATE OR REPLACE SEQUENCE ' || ?  ||'.'|| SEQUENCE_SCHEMA || '.' ||SEQUENCE_NAME || ' START WITH ' || START_VALUE || ' INCREMENT BY ' || \"INCREMENT\" || ';' FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.SEQUENCES WHERE SEQUENCE_SCHEMA = ?" + sequence_filter + " ORDER BY 1";
            var stmt = snowflake.createStatement({
                sqlText: sql_command,
                binds: [TARGET_DB, schema_name]
            });
            var res2 = stmt.execute();
            while (res2.next()) {
                row_as_json = res2.getColumnValue(1);
                // Add the row to the array of ddls.
                if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                    array_of_rows.push(row_as_json);
                    ++seq_num;
                }
            }
            break;		

        case "STREAMS":
            var streams_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " LIKE '" + OBJ_PATTERN + "%'";
            //Execute for stream
            snowflake.execute({
                sqlText: 'show streams' + streams_filter + ' IN ' + SOURCE_DB + '.' + schema_name + ';'
            });
            sql_command = "select * from table(result_scan(last_query_id())) order by \"name\";";
            var stmt = snowflake.createStatement({
                sqlText: sql_command
            });
            var res2 = stmt.execute();
            while (res2.next()) {
                var sliced_tblname = "";
                var name = res2.getColumnValue(2);
                var table_name = res2.getColumnValue(7);
                if (table_name === null || table_name === '')
                    continue;
                //if table_name is not fully qualified name, then create one
                //if table_name ends with . then skip this stream
                //if table_name is fully qualified then ensure it points to target database 
                if (table_name.lastIndexOf(".") == -1)
                    sliced_tblname = table_name;
                else if (table_name.length == table_name.lastIndexOf(".") + 1)
                    continue;
                else
                    sliced_tblname = table_name.substring(table_name.lastIndexOf(".") + 1);
                var src_tblname = TARGET_DB + "." + schema_name + "." + sliced_tblname;
                table_txt = table_name.replace(SOURCE_DB, TARGET_DB) === src_tblname ? " ON TABLE " + src_tblname : " ON TABLE " + table_name.replace(SOURCE_DB, TARGET_DB);                
                var mode_name = res2.getColumnValue(10);
                var mode_txt = mode_name === null || mode_name === '' || mode_name.toUpperCase() === 'DEFAULT' ? '' : " " + mode_name + " = TRUE;"
                var str_ddl = FIRST_RUN ?
                    "CREATE STREAM IF NOT EXISTS " + TARGET_DB + "." + schema_name + "." + name + table_txt + mode_txt :
                    "CREATE OR REPLACE STREAM " + TARGET_DB + "." + schema_name + "." + name + table_txt + mode_txt;
                //Add the stream definition to the array of commands
                if (str_ddl !== null && str_ddl.replace(/ /g, '') !== '') {
                    array_of_rows.push(str_ddl);
                    ++stream_num;
                }

            }
            break;

        case "TASK":
            //Execute for task 
            var tasks_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " LIKE '" + OBJ_PATTERN + "%'";
            snowflake.execute({
                sqlText: 'show tasks' + tasks_filter + ' IN ' + SOURCE_DB + '.' + schema_name + ';'
            });
            sql_command = `WITH recursive cte
                                            (
                                            created_on
                                            ,name
                                            ,id
                                            ,database_name
                                            ,schema_name
                                            ,OWNER
                                            ,comment
                                            ,warehouse
                                            ,schedule
                                            ,predecessors
                                            ,STATE
                                            ,DEFINITION
                                            ,condition
											,allow_overlapping_execution
                                            ,LEVEL
                                            )
                                        AS
                                            (
                                            SELECT \"created_on\"
                                                ,\"name\"
                                                ,\"id\"
                                                ,\"database_name\"
                                                ,\"schema_name\"
                                                ,\"owner\"
                                                ,\"comment\"
                                                ,\"warehouse\"
                                                ,\"schedule\"
                                                ,SPLIT(\"predecessors\", '.') [2] AS \"predecessors\"
                                                ,\"state\"
                                                ,\"definition\"
                                                ,\"condition\"
												,\"allow_overlapping_execution\"
                                                ,0 AS \"level\"
                                            FROM TABLE (result_scan(last_query_id()))
                                            WHERE NVL(SPLIT(\"predecessors\", '.') [2], 'NULL')  != all (select NVL(e.\"name\", 'NULL') from TABLE (result_scan(last_query_id())) e)
                                            
                                            UNION ALL
                                            
                                            SELECT A.\"created_on\"
                                                ,A.\"name\"
                                                ,A.\"id\"
                                                ,A.\"database_name\"
                                                ,A.\"schema_name\"
                                                ,A.\"owner\"
                                                ,A.\"comment\"
                                                ,A.\"warehouse\"
                                                ,A.\"schedule\"
                                                ,SPLIT(A.\"predecessors\", '.') [2] AS \"predecessors\"
                                                ,A.\"state\"
                                                ,A.\"definition\"
                                                ,A.\"condition\"
												,A.\"allow_overlapping_execution\"
                                                ,B.LEVEL + 1
                                            FROM TABLE (result_scan(last_query_id())) AS A
                                            INNER JOIN cte AS B ON REPLACE(SPLIT(A.\"predecessors\", '.') [2],'"','') = B.name
                                            )
                                        SELECT DISTINCT created_on
                                            ,name
                                            ,id
                                            ,database_name
                                            ,schema_name
                                            ,OWNER
                                            ,comment
                                            ,warehouse
                                            ,schedule
                                            ,predecessors
                                            ,STATE
                                            ,DEFINITION
                                            ,condition
											,allow_overlapping_execution
                                            ,LEVEL
                                        FROM cte
                                        ORDER BY LEVEL;`;
            var stmt = snowflake.createStatement({
                sqlText: sql_command
            });
            var res2 = stmt.execute();
            while (res2.next()) {
                var name = '"' + res2.getColumnValue(2) + '"';
				var comment = res2.getColumnValue(7);
                var wh = res2.getColumnValue(8);
                var schedule = res2.getColumnValue(9);
                var after = res2.getColumnValue(10);
                var definations = res2.getColumnValue(12);
                var conditions = res2.getColumnValue(13);
				var overlapping_execution = res2.getColumnValue(14);
                var schedule_text = schedule === null || schedule === '' ? '' : " schedule='" + schedule + "'";
				var comment_text = comment === null || comment === '' ? '' : " COMMENT='" + comment + "'";	
                var after_text = after === null || after === '' ? '' : " after  " + TARGET_DB + "." + schema_name + "." + after;
                var conditions_text = conditions === null || conditions === '' ? '' : " when   " + conditions + " ";
                var overlapping_execution_text = overlapping_execution === 'null' || overlapping_execution === '' ? '' : " ALLOW_OVERLAPPING_EXECUTION=" + overlapping_execution;
			   
                default_wh = TARGET_DB + '_LVW'
                sql_command = "show warehouses like '" + TARGET_DB + "%';"
                var stmt = snowflake.createStatement({ sqlText: sql_command });
                var res = stmt.execute();
                
                while (res.next()) 
                    {
                        var warehouse = res.getColumnValue(1);
                        if (warehouse === TARGET_DB + wh.substring(wh.indexOf(SOURCE_DB) + SOURCE_DB.length,wh.length)) { var target_wh = 'YES'; }
                        else if( warehouse === default_wh ) { var def_wh = 'YES'; }
                    }
                    
                if (TARGET_WH.trim().length > 0) { wh = TARGET_WH.trim().toUpperCase(); }    
                else if (target_wh === 'YES') { wh = TARGET_DB + wh.substring(wh.indexOf(SOURCE_DB) + SOURCE_DB.length,wh.length); }                 
                else if (def_wh === 'YES') { wh = default_wh; }
				
				// Handle Session Parameters for Task 
                var session_parameter_text = "";				
				parm_command = "SHOW PARAMETERS IN TASK " + SOURCE_DB + '.' + schema_name + '.' + name + ";";
				var cmd = snowflake.createStatement({ sqlText: parm_command });		
				var res_set = cmd.execute();    	 	   
				   
				if (res_set.next()) { 		
				if (res_set.getColumnValue(4) === "TASK") {
				   if (res_set.getColumnValue(6) === "STRING")       
				      { session_parameter_text += res_set.getColumnValue(1) + "=" + "'" + res_set.getColumnValue(2) + "'" + " "} 
						   else { session_parameter_text += res_set.getColumnValue(1) + "=" + res_set.getColumnValue(2) + " " } 
					  }
					 }		   
						  
				while (res_set.next()) {
				 if (res_set.getColumnValue(4) === "TASK") {
				  if (res_set.getColumnValue(6) === "STRING")       
					{ session_parameter_text += res_set.getColumnValue(1) + "=" + "'" + res_set.getColumnValue(2) + "'" + " "} 
					  else { session_parameter_text += res_set.getColumnValue(1) + "=" + res_set.getColumnValue(2) + " "}       
					 	  }
				    }

                if (session_parameter_text.trim().length > 0) { session_parameter_text = " " + session_parameter_text; }
				                
                var str_ddl = FIRST_RUN ?
                    "CREATE TASK IF NOT EXISTS " + TARGET_DB + "." + schema_name + "." + name + " warehouse= " + wh + schedule_text + session_parameter_text + comment_text + overlapping_execution_text + after_text + conditions_text + " as " + definations + " ;" :
                    "CREATE OR REPLACE TASK  " + TARGET_DB + "." + schema_name + "." + name + " warehouse= " + wh + schedule_text + session_parameter_text + comment_text + overlapping_execution_text + after_text + conditions_text + " as " + definations + " ;";
                //Add the task definition to the array of commands
                if (str_ddl !== null && str_ddl.replace(/ /g, '') !== '') {
                    array_of_rows.push(str_ddl);
                    ++task_num;
                }

            }
            break;
    }

    schema_num++;
});

//set execution environment to TARGET_DB to execute all the ddls 
snowflake.execute({
    sqlText: 'use database ' + TARGET_DB + ';'
});

// TODO - Check for Overflow
// Iterate through list of create statements
for (var key in array_of_rows) {
    var result = "";
    st = array_of_rows[key];
    if (st !== null && st.replace(/ /g, '') !== '') {
        var stmt = snowflake.createStatement({
            sqlText: st //,
            //binds:[TARGET_DB,schema_name]
        });
    }
    // Execute Create statement and if sucsesfull - prepare positive response, and if not - error details as response.
    try {
        stmt.execute();
        result = "Succeeded";
    } catch (err) {
        result = "Failed: Code: " + err.code + "\\n  State: " + err.state;
        result += "\\n  Message: " + err.message;
        result += "\\nStack Trace:\\n" + err.stackTraceTxt;
    }
   
    if (res_message.trim().length > 0) 
         { report_rows.push({
            "Statement": st,
            "Result": result,
            "Warning": res_message			
		        }) }
            else { report_rows.push({
					"Statement": st,
					"Result": result
					   })}; //Pushes the result of the statement and the original statement to the log.
}

// Put the array in a JSON variable (so it looks like a VARIANT to
// Snowflake).  The key is "Clones", and the value is the array that has
// the rows we want.
table_as_json = {
    "Src Tables Count": table_num,
    "Src Views Count": view_num,
    "Src FF Count": ff_num,
    "Src Stages Count": stage_num,
    "Src SPs Count": sp_num,
    "Src Funcs Count": fun_num,
    "Src Streams Count": stream_num,
    "Src Tasks Count": task_num,
	"Src Sequences Count": seq_num,
    "Clones": report_rows
};

// Return the rows to Snowflake, which expects a JSON-compatible VARIANT.
return table_as_json;
$$;