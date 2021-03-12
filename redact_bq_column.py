import json
from google.cloud import bigquery

### GLOBAL VARIABLES
project_id = "lithe-record-303112"
dataset_id = "mydataset"

def get_dml_schema (path_to_schema_json_file):
    with open(path_to_schema_json_file) as f:
        bigqueryColumns = json.load(f)
        schema = ""
        for col in bigqueryColumns:
            mode = "NOT NULL" if (col.get('mode') == "REQUIRED") else ""
            schema += "`" + col['name'] + "` " + col['type'] + " " + mode + " OPTIONS(description=\"No description\"),\n"
    return schema

def redact_column(table_name, column_name_to_redact, project_id=project_id, dataset_id=dataset_id):
    schema = get_dml_schema(f"{table_name}.json")

    dml_query = f'''CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{table_name}`
    (
    {schema}
    ) 
    AS
    SELECT * EXCEPT({column_name_to_redact}), "CONFIDENTIAL" AS {column_name_to_redact}
    FROM `{project_id}.{dataset_id}.{table_name}`
    '''

    return dml_query
    # query_job = bigquery.Client().query(dml_query)
    # return query_job.result() # blocks until bq job is complete

if __name__ == "__main__":
    print(redact_column("mytable", "number"))
