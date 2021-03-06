import json, os
from google.cloud import bigquery

### GLOBAL VARIABLES
project_id = "lithe-record-303112"
dataset_id = "my_destination_dataset"

def prompt():
    while (True):
        cin = input("Quality check failed! Enter Y if you'd like to continue or N to terminate program: ")
        if cin == "Y":
            break
        elif cin == "N":
            raise Exception("Program terminated") 

def get_table(table_name, project_id=project_id, dataset_id=dataset_id):
    client = bigquery.Client()

    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = dataset_ref.table(table_name)
    table = client.get_table(table_ref)

    return table

def get_dml_schema (path_to_schema_json_file):
    with open(path_to_schema_json_file) as f:
        bigqueryColumns = json.load(f)
        schema = ""
        for col in bigqueryColumns:
            mode = "NOT NULL" if (col.get('mode') == "REQUIRED") else ""
            schema += col['name'] + " " + col['type'] + " " + mode + ",\n"
    return schema

def fix_schema(table_name, project_id=project_id, dataset_id=dataset_id):

    schema = get_dml_schema(f"schemas/{table_name}.json")

    dml_query = f'''CREATE OR REPLACE TABLE {project_id}.{dataset_id}.{table_name}
    (
    {schema}
    ) 
    OPTIONS(description="No description")
    AS
    SELECT *
    FROM {project_id}.{dataset_id}.{table_name}
    '''

    query_job = bigquery.Client().query(dml_query)
    return query_job.result() # blocks until bq job is complete

if __name__ == "__main__":

    schema_file_names = os.listdir('./schemas')

    for schema_filename in schema_file_names:
        table_name = schema_filename[:-5]

        num_bytes_before, num_rows_before = get_table(table_name).num_bytes, get_table(table_name).num_rows # qa

        response = fix_schema(table_name)
        print(response) # qa

        num_bytes_after, num_rows_after = get_table(table_name).num_bytes, get_table(table_name).num_rows # qa

        if num_bytes_before == num_bytes_after and num_rows_before == num_rows_after: # qa
            print("SUCCESSFUL!") # qa
        else:
            print(num_bytes_before, num_bytes_after, num_rows_before, num_rows_after) # qa
            prompt()
    
