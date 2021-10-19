create or replace procedure EDW_OPS.OPS.clone_db_objects_adv(SOURCE_DB VARCHAR, TARGET_DB VARCHAR, OBJ_PATTERN VARCHAR, ADV_FILTER VARCHAR, TARGET_WH VARCHAR)
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
//                          : The script takes care of tasks-to-task dependency
//                          : The script does not take care of view-to-view dependency currently
//							
//- Parameters              : 1) <SOURCE_DB> - The source Database name
//                            2) <TARGET_DB> - The target Database name
//                            3) <OBJECT_PATTERN> - Project name (or any text pattern in general) which is matched as prefix against the DB object names
//                            4) <ADV_FILTER> - A JSON string specifying the schema and the  specific set of DB objects to be moved (If Empty, the basic migration SP will be called internally)
//                                           Ex. {
//                                                  "STG": {
//                                                      "SCHEMA": "STG",
//                                                      "TABLES": [{"name":"ACCOUNT_ACCOUNT_NUMBER","movestructureonly":true,"preserverdata":false},{BAK_STG_Y5PPLZA0,"movestructureonly":true,"preserverdata":false}],
//                                                      "VIEWS": "COMPANY_CODE_SV",
//                                                      "FF": "",
//                                                      "STAGES": "",
//                                                      "SP": "CLONE_DB_OBJECTS_BASIC,CLONE_DB_OBJECTS_ADVANCED",
//                                                      "FUNCTIONS": " ",
//                                                      "STREAMS": "STG_0APO_LOCNO_S,STG_0APO_LPROD_S",
//                                                      "TASKS": "ELT_CONTROL_TABLE_TASK,GRP_1_EVENT_TRIGGER_TASK"
//                                                      "SEQUENCES": "PDH_FILE_NUMBER,PDH_SEQ_FULLLOAD"
//                                                  }
//                                              }
//                            5) <TARGET_WH> - Warehouse name to be assigned to the tasks in target
//- Returns                 : Execution log in the form of JSON
//------------------------------------------------------------------------------------------------------------------------//


// This array will contain all the rows.
var array_of_rows = [];
// This variable will hold a JSON data structure that we can return as a VARIANT.
// This will contain ALL the rows in a single "value".
var table_as_json = {};
//execution report holder
var report_rows = [];
// Advanced Schema Filter handling
var adv_filter = ADV_FILTER.trim();

// Following vars carry extracted info from the ADV_FILTER 
var schema_names = [];
var table_names = [];
var view_names = [];
var sp_names = [];
var func_names = [];
var stage_names = [];
var ff_names = [];
var stream_names = [];
var task_names = [];
var seq_names = [];

var table_name_filter = "";
var view_name_filter = "";
var sp_name_filter = "";
var func_name_filter = "";
var stage_name_filter = "";
var ff_name_filter = "";
var stream_name_filter = "";
var task_name_filter = "";
var seq_name_filter = "";

var table_num = 0;
var view_num = 0;
var ff_num = 0;
var stage_num = 0;
var sp_num = 0;
var func_num = 0;
var stream_num = 0;
var task_num = 0;
var seq_num = 0;

var preservedata_var;
var movestructure_var;

var res_message = "";


if (adv_filter === '' || adv_filter === null || adv_filter === 'ALL') {
    //SOURCE_DB , TARGET_DB , OBJ_PATTERN , SCHEMA_FILTER , OBJECT_TYPE , FIRST_RUN , MOVE_TABLE_STRUCTURE_ONLY , PRESERVE_TARGET_TABLE_DATA ,TARGET_WH)
    return "Advanced filter is missing.Please pass advanced filter in right format to proceed with migration";
    //Below piece of code will not be executed until above return is removed. This is intentionally placed here so that this 
    //can be handled in future
    var stmt = snowflake.createStatement({
        sqlText: 'CALL EDW_OPS.OPS.CLONE_DB_OBJECTS(?, ?, ?, ?, ?, ?, ?, ?, ?)',
        binds: [SOURCE_DB, TARGET_DB, OBJ_PATTERN, "", "", 1, 1, 0, TARGET_WH]
    });
    table_as_json = stmt.execute();
    return table_as_json;
}
else {
    // Parse the string into JSON object 
    var parsedJSON = JSON.parse(adv_filter);

    // Split the comma delimited names of db objects into corresponding lists for each schema
    Object.keys(parsedJSON).forEach(function (schema) {
        schema_names.push("(" + schema.trim().toUpperCase() + ")");

        //set execution environment to SOURCE_DB to read all the ddls 
        snowflake.execute({
            sqlText: 'use database ' + SOURCE_DB + ';'
        });
        snowflake.execute({
            sqlText: 'use schema ' + schema + ';'
        });

        var views = parsedJSON[schema].VIEWS.replace(/ /g, '').split(",");
        var sps = parsedJSON[schema].SP.replace(/ /g, '').split(",");
        var funcs = parsedJSON[schema].FUNCTIONS.replace(/ /g, '').split(",");
        var stages = parsedJSON[schema].STAGES.replace(/ /g, '').split(",");
        var ffs = parsedJSON[schema].FF.replace(/ /g, '').split(",");
        var streams = parsedJSON[schema].STREAMS.replace(/ /g, '').split(",");
        var tasks = parsedJSON[schema].TASKS.replace(/ /g, '').split(",");               
        var tbls = parsedJSON[schema].TABLES;
		var seqes = parsedJSON[schema].SEQUENCES.replace(/ /g, '').split(","); 

        for (var key in tbls) {
            if (tbls.length === 1 && tbls[0].name.trim().toUpperCase() === "ALL") {
                var stmt = snowflake.createStatement({
                    sqlText: "select table_name from " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + schema + "' AND Table_type = 'BASE TABLE' ;"
                });
                var res = stmt.execute();
                while (res.next()) {
                    table_names.push("'" + res.getColumnValue(1) + "'");
                }
            } else {
                tbls.forEach(function (t) {
                    if (t.name !== null && t.name.replace(/ /g, '') !== '') {
                        tbl = "'" + t.name.trim().toUpperCase() + "'";
                        table_names.push(tbl);
                    }
                });
            }
        }

        if (views.length === 0 || (views.length === 1 && views[0].trim().toUpperCase() === "ALL")) {
            var stmt = snowflake.createStatement({
                sqlText: "select split(split(view_definition, '" + SOURCE_DB + "." + schema + ".')[1], ' ')[0] AS view_name from " + SOURCE_DB + ".INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = " + schema + " AND VIEW_DEFINITION IS NOT NULL;"
            });
            var res = stmt.execute();
            while (res.next()) {
                view_names.push("'" + res.getColumnValue(1) + "'");
            }
        } else {
            views.forEach(function (v) {
                if (v !== null && v.replace(/ /g, '') !== '') {
                    v = "'" + v.trim().toUpperCase() + "'";
                    view_names.push(v);
                }

            });
        }

        if (sps.length === 0 || (sps.length === 1 && sps[0].trim().toUpperCase() === "ALL")) {
            var stmt = snowflake.createStatement({
                sqlText: "select procedure_name from " + SOURCE_DB + ".INFORMATION_SCHEMA.PROCEDURES where PROCEDURE_SCHEMA = " + schema + ";"
            });
            var res = stmt.execute();
            while (res.next()) {
                sp_names.push("'" + res.getColumnValue(1) + "'");
            }
        } else {
            sps.forEach(function (s) {
                if (s !== null && s.replace(/ /g, '') !== '') {
                    s = "'" + s.trim().toUpperCase() + "'";
                    sp_names.push(s);
                }

            });
        }

        if (funcs.length === 0 || (funcs.length === 1 && funcs[0].trim().toUpperCase() === "ALL")) {
            var stmt = snowflake.createStatement({
                sqlText: "select function_name from " + SOURCE_DB + ".INFORMATION_SCHEMA.FUNCTIONS where FUNCTION_SCHEMA = " + schema + ";"
            });
            var res = stmt.execute();
            while (res.next()) {
                func_names.push("'" + res.getColumnValue(1) + "'");
            }
        } else {
            funcs.forEach(function (f) {
                if (f !== null && f.replace(/ /g, '') !== '') {
                    f = "'" + f.trim().toUpperCase() + "'";
                    func_names.push(f);
                }

            });
        }
		
		if (seqes.length === 0 || (seqes.length === 1 && seqes[0].trim().toUpperCase() === "ALL")) {
            var stmt = snowflake.createStatement({
                sqlText: "SELECT SEQUENCE_NAME FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.SEQUENCES WHERE SEQUENCE_SCHEMA = " + schema + ";"
            });
            var res = stmt.execute();
            while (res.next()) {
                seq_names.push("'" + res.getColumnValue(1) + "'");
            }
        } else {
            seqes.forEach(function (q) {
                if (q !== null && q.replace(/ /g, '') !== '') {
                    q = "'" + q.trim().toUpperCase() + "'";
                    seq_names.push(q);
                }

            });
        }		

        if (stages.length === 0 || (stages.length === 1 && stages[0].trim().toUpperCase() === "ALL")) {
            var stmt = snowflake.createStatement({
                sqlText: "select stage_name from " + SOURCE_DB + ".INFORMATION_SCHEMA.STAGES where STAGE_SCHEMA = " + schema + ";"
            });
            var res = stmt.execute();
            while (res.next()) {
                stage_names.push("'" + res.getColumnValue(1) + "'");
            }
        } else {
            stages.forEach(function (stg) {
                if (stg !== null && stg.replace(/ /g, '') !== '') {
                    stg = "'" + stg.trim().toUpperCase() + "'";
                    stage_names.push(stg);
                }

            });
        }

        if (ffs.length === 0 || (ffs.length === 1 && ffs[0].trim().toUpperCase() === "ALL")) {
            var stmt = snowflake.createStatement({
                sqlText: "select file_format_name from " + SOURCE_DB + ".INFORMATION_SCHEMA.FILE_FORMATS where FILE_FORMAT_SCHEMA = " + schema + ";"
            });
            var res = stmt.execute();
            while (res.next()) {
                ff_names.push("'" + res.getColumnValue(1) + "'");
            }
        } else {
            ffs.forEach(function (ff) {
                if (ff !== null && ff.replace(/ /g, '') !== '') {
                    ff = "'" + ff.trim().toUpperCase() + "'";
                    ff_names.push(ff);
                }

            });
        }

        if (streams.length === 0 || (streams.length === 1 && streams[0].trim().toUpperCase() === "ALL")) {
            snowflake.execute({
                sqlText: "show streams IN " + SOURCE_DB + "." + schema + ";"
            });
            var stmt = snowflake.createStatement({
                sqlText: "select \"name\" from table(result_scan(last_query_id()));"
            });
            var res = stmt.execute();
            while (res.next()) {
                stream_names.push("'" + res.getColumnValue(1) + "'");
            }
        } else {
            streams.forEach(function (str) {
                if (str !== null && str.replace(/ /g, '') !== '') {
                    str = "'" + str.trim().toUpperCase() + "'";
                    stream_names.push(str);
                }

            });
        }

        if (tasks.length === 0 || (tasks.length === 1 && tasks[0].trim().toUpperCase() === "ALL")) {
            var stmt = snowflake.createStatement({
                sqlText: `with recursive cte
								(name, predecessors, level) 
								as 
								(
									select
										\"name\", 
										\"predecessors\",
										0 AS \"level\"
									from TABLE(result_scan(last_query_id()))
									where SPLIT(\" predecessors\", '.') [2] IS NULL

									union

									select A.\"name\", 
										A.\"predecessors\"
										,B.level + 1
									from TABLE(result_scan(last_query_id())) AS A join cte AS B
										on SPLIT(A.\"predecessors\", '.') [2] = B.name
								)
							select distinct
									name,
									predecessors,
									level
								from cte
								order by level;`
            });
            var res = stmt.execute();
            while (res.next()) {
                task_names.push("'" + res.getColumnValue(1) + "'");
            }
        } else {
            tasks.forEach(function (tsk) {
                if (tsk !== null && tsk.replace(/ /g, '') !== '') {
                    tsk = "'" + tsk.trim().toUpperCase() + "'";
                    task_names.push(tsk);
                }

            });
        }

        table_name_filter = "(" + table_names.join(', ') + ")";
        view_name_filter = "(" + view_names.join(', ') + ")";
        sp_name_filter = "(" + sp_names.join(', ') + ")";
        func_name_filter = "(" + func_names.join(', ') + ")";
        stage_name_filter = "(" + stage_names.join(', ') + ")";
        ff_name_filter = "(" + ff_names.join(', ') + ")";
        stream_name_filter = "(" + stream_names.join(', ') + ")";
        task_name_filter = "(" + task_names.join(', ') + ")";
		seq_name_filter = "(" + seq_names.join(', ') + ")";


        //Execute for Tables
        //create a list of execute statements utilizing zero copy clone
        if (table_names.length !== 0) {
            var table_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " AND TABLE_NAME LIKE '" + OBJ_PATTERN + "%'";
            sql_command = "select 'CREATE or REPLACE ' || SPLIT_part(TABLE_TYPE,' ',-1) || ' '||?||  '.'||table_schema||'.\"'|| table_name ||'\" clone '||table_catalog||'.'||table_schema||'.\"'||table_name||'\";' clone_statement,table_name,IS_TRANSIENT FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.TABLES WHERE   TABLE_SCHEMA =?  AND  Table_type = 'BASE TABLE'" + table_filter + " AND TABLE_NAME IN " + table_name_filter;
            var stmt = snowflake.createStatement({
                sqlText: sql_command,
                binds: [TARGET_DB, schema]
            });
            var res2 = stmt.execute();
            // Iterate through tables and add them to the list of commands to execute 
            while (res2.next()) {
                row_as_json = res2.getColumnValue(1);
                tbl_nm = res2.getColumnValue(2);
                is_transient = res2.getColumnValue(3);
                for (var prop in tbls) {
                    if (tbls[prop].name === tbl_nm || tbls[prop].name === "ALL" ) {
                        movestructure_var = tbls[prop].movestructureonly;
                        preservedata_var = tbls[prop].preservedata;
                    }                    
                }
                // Add the row to the array of rows.
                if (is_transient == 'NO') {
                    if (preservedata_var) {
                        sql_command = `SELECT COUNT(*)
                                            FROM ` + TARGET_DB + `.INFORMATION_SCHEMA.TABLES
                                            WHERE Table_type = 'BASE TABLE'
                                                AND TABLE_SCHEMA = '` + schema + `'
                                                AND ROW_COUNT <> 0
                                                AND TABLE_NAME = '` + tbl_nm + `'`;
                        var resCount = snowflake.execute({
                            sqlText: sql_command
                        });
                        resCount.next();
                        var count = resCount.getColumnValue(1);
                        //existing target count>0; replace existing bkp table; else bkp is not replaced 
                        if (count !== null && count !== '' && count !== 0) {
                            bkp_tbl = `CREATE OR REPLACE TABLE ` + TARGET_DB + `.` + schema + `.\"` + tbl_nm + `_bkp\"  clone ` + TARGET_DB + `.` + schema + `.\"` + tbl_nm + `\";`;                            
                            array_of_rows.push(bkp_tbl);
                        }
                    }
                    if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                        array_of_rows.push(row_as_json);
                        ++table_num;
                    }
                    if (movestructure_var) {
                        array_of_rows.push("TRUNCATE TABLE IF EXISTS " + TARGET_DB + "." + schema + ".\"" + tbl_nm + "\";");
                    }
                }
                else {
                    var get_ddl_tbl = "SELECT GET_DDL('TABLE','" + SOURCE_DB + "." + schema + "." + tbl_nm + "');";
                    var stmt = snowflake.createStatement({ sqlText: get_ddl_tbl });
                    var res_ddl = stmt.execute();
                    res_ddl.next();
                    tbl_q = res_ddl.getColumnValue(1);
                    var str = tbl_q.indexOf(schema + "." + tbl_nm) > 1 ? tbl_q : tbl_q.replace(tbl_nm, schema + "." + tbl_nm);
                    array_of_rows.push(str);
                }
            }
        }

        if (view_names.length !== 0) {
            //Execute for Views
            var view_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " AND TABLE_NAME LIKE '" + OBJ_PATTERN + "%'";
            sql_command = "SELECT TABLE_NAME, VIEW_DEFINITION from " + SOURCE_DB + ".INFORMATION_SCHEMA.VIEWS where VIEW_DEFINITION IS NOT NULL AND TABLE_SCHEMA = '" + schema + "'" + view_filter + " AND TABLE_NAME IN " + view_name_filter;
            var res2 = snowflake.execute({
                sqlText: sql_command
            });
            while (res2.next()) {
                var view = res2.getColumnValue(1);             
                row_as_json = res2.getColumnValue(2);
                var str = row_as_json.indexOf(schema + "." + view) > 1 ? row_as_json : row_as_json.replace(view, schema + "." + view);
                // Add the row to the array of rows.
                if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                    array_of_rows.push(str);
                    ++view_num;
                }                
            }
        }

        if (ff_names.length !== 0) {
            //Execute for FILE_FORMATS
            //File format is a logical object, therefore cannot be cloned. can only be redeployed 
            var file_format_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " AND FILE_FORMAT_NAME LIKE '" + OBJ_PATTERN + "%'";
            sql_command = "select FILE_FORMAT_catalog || '.' || FILE_FORMAT_schema || '.' ||  FILE_FORMAT_name || '' , FILE_FORMAT_name  FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.FILE_FORMATS WHERE FILE_FORMAT_SCHEMA =  ?" + file_format_filter + " AND FILE_FORMAT_NAME IN " + ff_name_filter;

            var stmt = snowflake.createStatement({
                sqlText: sql_command,
                binds: [schema]
            });
            var res2 = stmt.execute();
            //Iterate throuth all the file formats 
            while (res2.next()) {
                file_form = res2.getColumnValue(1);
                file_format = res2.getColumnValue(2);
                // Extract a format definition
                sql_command = "select get_ddl('file_format',?)";
                var stmt = snowflake.createStatement({
                    sqlText: sql_command,
                    binds: [file_form]
                });
                var resf = stmt.execute();
                resf.next();
                // Modify in case of full qualified names
                row_as_json = resf.getColumnValue(1);
                row_as_json.replace(SOURCE_DB, TARGET_DB);
                //tbl_q = res_ddl.getColumnValue(1);
                var str = row_as_json.indexOf(schema + ".") > 1 ? row_as_json : row_as_json.replace(file_format, schema + "." + file_format);

                // Add the row to the array of commands.
                /* if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                     array_of_rows.push(row_as_json);
                     ++ff_num;
                 }*/
                if (str !== null && str.replace(/ /g, '') !== '') {
                    array_of_rows.push(str);
                    ++ff_num;
                }
            }
        }

        if (stage_names.length !== 0) {
            //Execute for STAGES
            var stage_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " AND STAGE_NAME LIKE '" + OBJ_PATTERN + "%'";
            sql_command = "select 'CREATE or REPLACE stage '  || ? || '.' ||STAGE_schema|| '.' || STAGE_name ||' clone '||STAGE_catalog||'.'||STAGE_schema||'.'||STAGE_name||';' clone_statement FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.STAGES WHERE STAGE_SCHEMA = ?" + stage_filter + " AND STAGE_NAME IN " + stage_name_filter;
            var stmt = snowflake.createStatement({
                sqlText: sql_command,
                binds: [TARGET_DB, schema]
            });
            var res2 = stmt.execute();
            while (res2.next()) {
                row_as_json = res2.getColumnValue(1);
                // Add the row to the array of rows.
                if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                    array_of_rows.push(row_as_json);
                    ++stage_num;
                }
            }
        }

 		if (sp_names.length !== 0) {
		    //Execute for PROCEDURES
			//procedure statement is redesigned to fit the target database based on the metadata stored in procedures repository.
			var procedure_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " AND PROCEDURE_NAME LIKE '" + OBJ_PATTERN + "%'";
            sql_command = "SELECT PROCEDURE_NAME, REGEXP_REPLACE(ARGUMENT_SIGNATURE, '([(,]\\\\s*)\\\\w*\\\\s*(\\\\w*\\\\)?)', '\\\\1\\\\2') FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.PROCEDURES WHERE PROCEDURE_SCHEMA = ?" + procedure_filter + " AND PROCEDURE_NAME IN " + sp_name_filter;
            						
			var stmt = snowflake.createStatement({
                sqlText: sql_command,
                binds: [schema]			
                  });		
		
            var res2 = stmt.execute();
            while (res2.next()) {
                var proc_name = res2.getColumnValue(1);				
			    var arg_name = res2.getColumnValue(2);						
			    des_command = "DESCRIBE PROCEDURE " + proc_name + arg_name +";";
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
                  owner_command = "SELECT 1 FROM EDW_OPS.INT.OWNER_RIGHT_PROCS WHERE PROCEDURE_NAME = '" + proc_name + arg_name + "' AND PROCEDURE_SCHEMA = '" + schema + "';"; 
				  var stmt_owner = snowflake.createStatement({ sqlText: owner_command });
				  var res4 = stmt_owner.execute();
				  execute_as_text = "CALLER";
				  res_message = "Procedure " + proc_name + arg_name + " migrated with 'EXECUTE AS CALLER', where as in source database created with 'OWNER'";
				
				while (res4.next()) {					
				   if (res4.getColumnValue(1) === 1) {execute_as_text = "OWNER" ; res_message = "";}
				           }
				      }               
                
                var str_ddl = "CREATE OR REPLACE PROCEDURE " + TARGET_DB + "." + schema + "." + proc_name + signature_text + " RETURNS " + returns_text + " LANGUAGE " + language_text + " EXECUTE AS " + execute_as_text + " AS " + "\$\$ " + body_text + " \$\$;";
				   
				if (str_ddl !== null && str_ddl.replace(/ /g, '') !== '') {
                    array_of_rows.push(str_ddl);
                    ++sp_num;
                  }
				}
		      }				

        if (func_names.length !== 0) {
            //Execute for FUNCTIONS
            var function_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " AND FUNCTION_NAME LIKE '" + OBJ_PATTERN + "%'";
            sql_command = "select 'CREATE or REPLACE FUNCTION '  || ? ||'.'||FUNCTION_schema||'.'||FUNCTION_name  ||  ARGUMENT_SIGNATURE || ' RETURNS ' || DATA_TYPE || ' LANGUAGE JAVASCRIPT AS \$\$ ' || FUNCTION_DEFINITION || ' \$\$;' clone_statement  FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.FUNCTIONS WHERE FUNCTION_SCHEMA = ?" + function_filter;
            var stmt = snowflake.createStatement({
                sqlText: sql_command,
                binds: [TARGET_DB, schema]
            });
            var res2 = stmt.execute();
            while (res2.next()) {
                row_as_json = res2.getColumnValue(1);
                // Add the row to the array of rows.
                if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                    array_of_rows.push(row_as_json);
                    ++func_num;
                }
            }
        }
		
		if (seq_names.length !== 0) {		
		    //Execute for SEQUENCE
            var sequence_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " AND SEQUENCE_NAME LIKE '" + OBJ_PATTERN + "%'";
            sql_command = "SELECT 'CREATE OR REPLACE SEQUENCE ' || ?  ||'.'|| SEQUENCE_SCHEMA || '.' ||SEQUENCE_NAME || ' START WITH ' || START_VALUE || ' INCREMENT BY ' || \"INCREMENT\" || ';' FROM " + SOURCE_DB + ".INFORMATION_SCHEMA.SEQUENCES WHERE SEQUENCE_SCHEMA = ?" + sequence_filter + " AND SEQUENCE_NAME IN " + seq_name_filter + " ORDER BY 1";
            var stmt = snowflake.createStatement({
                sqlText: sql_command,
                binds: [TARGET_DB, schema]
            });
            var res2 = stmt.execute();
            while (res2.next()) {
                row_as_json = res2.getColumnValue(1);
                // Add the row to the array of rows.
                if (row_as_json !== null && row_as_json.replace(/ /g, '') !== '') {
                    array_of_rows.push(row_as_json);
                    ++seq_num;
                }
            }
		}	

        var streams_tasks_filter = OBJ_PATTERN === null || OBJ_PATTERN === '' ? '' : " LIKE '" + OBJ_PATTERN + "%'";

        if (stream_names.length !== 0) {
            //Execute for stream
            snowflake.execute({
                sqlText: 'show streams' + streams_tasks_filter + ' IN ' + SOURCE_DB + '.' + schema + ';'
            });
            sql_command = "select * from table(result_scan(last_query_id())) where \"name\" IN " + stream_name_filter;
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
                //var table_txt = " ON TABLE " + table_name
                //if table_name is not fully qualified name, then create one
                //if table_name ends with . then skip this stream
                //if table_name is fully qualified then ensure it points to target database 
                if (table_name.lastIndexOf(".") == -1)
                    sliced_tblname = table_name;
                else if (table_name.length == table_name.lastIndexOf(".") + 1)
                    continue;
                else
                    sliced_tblname = table_name.substring(table_name.lastIndexOf(".") + 1);
                var src_tblname = TARGET_DB + "." + schema + "." + sliced_tblname;
                table_txt = table_name.replace(SOURCE_DB, TARGET_DB) === src_tblname ? " ON TABLE " + src_tblname : " ON TABLE " + table_name.replace(SOURCE_DB, TARGET_DB);
                var mode_name = res2.getColumnValue(10);
                var mode_txt = mode_name === null || mode_name === '' || mode_name.toUpperCase() === 'DEFAULT' ? '' : " " + mode_name + " = TRUE;"
                var str_ddl = " CREATE OR REPLACE STREAM " + TARGET_DB + "." + schema + "." + name + table_txt + mode_txt;
                //Add the stream definition to the array of commands
                if (str_ddl !== null && str_ddl.replace(/ /g, '') !== '') {
                    array_of_rows.push(str_ddl);
                    ++stream_num;
                }
            }
        }

        if (task_names.length !== 0) {
            //Execute for task 
            snowflake.execute({
                sqlText: 'show tasks' + streams_tasks_filter + ' IN ' + SOURCE_DB + '.' + schema + ';'
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
									//WHERE SPLIT(\"predecessors\", '.') [2] IS NULL
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
								WHERE name IN ` + task_name_filter + " ORDER BY LEVEL;";
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
                var after_text = after === null || after === '' ? '' : " after  " + TARGET_DB + "." + schema + "." + after;
                var conditions_text = conditions === null || conditions === '' ? '' : " when   " + conditions + " ";
				var overlapping_execution_text = overlapping_execution === 'null' || overlapping_execution === '' ? '' : "ALLOW_OVERLAPPING_EXECUTION=" + overlapping_execution;
                
                default_wh = TARGET_DB + '_LVW'
                sql_command = "show warehouses like '" + TARGET_DB + "%';"
                var stmt = snowflake.createStatement({ sqlText: sql_command });
                var res = stmt.execute();

                while (res.next()) {
                    var warehouse = res.getColumnValue(1);
                    if (warehouse === TARGET_DB + wh.substring(wh.indexOf(SOURCE_DB) + SOURCE_DB.length, wh.length)) { var target_wh = 'YES'; }
                    else if (warehouse === default_wh) { var def_wh = 'YES'; }
                }

                if (TARGET_WH.trim().length > 0) { wh = TARGET_WH.trim().toUpperCase(); }
                else if (target_wh === 'YES') { wh = TARGET_DB + wh.substring(wh.indexOf(SOURCE_DB) + SOURCE_DB.length, wh.length); }
                else if (def_wh === 'YES') { wh = default_wh; }
				
				// Handle Session Parameters for Task
                var session_parameter_text = "";				
				parm_command = "SHOW PARAMETERS IN TASK " + SOURCE_DB + '.' + schema + '.' + name + ";";
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

                var str_ddl = " CREATE OR REPLACE TASK  " + TARGET_DB + "." + schema + "." + name + " warehouse= " + wh + schedule_text + session_parameter_text + comment_text + overlapping_execution_text + after_text + conditions_text +
                    " as " + definations + " ; ";					           
                //Add the task definition to the array of commands
                if (str_ddl !== null && str_ddl.replace(/ /g, '') !== '') {
                    array_of_rows.push(str_ddl);
                    ++task_num;
                }
            }
        }
        table_names = [];        
    });

    //set execution environment to TARGET_DB to execute all the ddls 
    snowflake.execute({
        sqlText: 'use database ' + TARGET_DB + ';'
    });
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
            result = "Failed: Code: " + err.code + "\n  State: " + err.state;
            result += "\n  Message: " + err.message;
            result += "\nStack Trace:\n" + err.stackTraceTxt;
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
        // 
    }
    // Put the array in a JSON variable (so it looks like a VARIANT to
    // Snowflake).  The key is "Clones", and the value is the array that has
    // the rows we want.
    table_as_json = {
        "Src Tables": table_name_filter,
        "Src Tables Count": table_num,
        "Src Views": view_name_filter,
        "Src Views Count": view_num,
        "Src FF": ff_name_filter,
        "Src FF Count": ff_num,
        "Src Stages": stage_name_filter,
        "Src Stages Count": stage_num,
        "Src SPs": sp_name_filter,
        "Src SPs Count": sp_num,
        "Src Funcs": func_name_filter,
        "Src Funcs Count": func_num,
        "Src Streams": stream_name_filter,
        "Src Streams Count": stream_num,
        "Src Tasks": task_name_filter,
        "Src Tasks Count": task_num,
        "Src Sequences": seq_name_filter,
		"Src Sequences Count": seq_num,
        "Clones": report_rows,
    };

    // Return the rows to Snowflake, which expects a JSON-compatible VARIANT.
    return table_as_json;
}
$$
;