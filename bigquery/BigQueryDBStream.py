import copy
import os
import dbstream
import time
import google.cloud.bigquery

from bigquery.core.tools.print_colors import C
from bigquery.core.Table import create_table, create_columns
from config.google.google_auth import google_auth


class BigQueryDBStream(dbstream.DBStream):
    def __init__(self, instance_name, client_id):
        super().__init__(instance_name, client_id=client_id)
        self.instance_type_prefix = "BIGQ"
        self.ssh_init_port = 6543

    def connection(self):
        try:
            con = google.cloud.bigquery.client.Client(project=os.environ["BIG_QUERY_PROJECT_ID"], credentials=google_auth.credentials())
        except google.cloud.bigquery.dbapi.OperationalError:
            time.sleep(2)
            if self.ssh_tunnel:
                self.ssh_tunnel.close()
                self.create_tunnel()
            con = google.cloud.bigquery.client.Client()
        return con

    def _execute_query_custom(self, query):
        con = self.connection()
        query_job = con.query(query)
        r = query_job.result()
        result = r.to_dataframe().to_dict(orient='records')
        return result

    def _send(self, data, replace, batch_size=1000):
        print(C.WARNING + "Initiate send_to_bigquery on table " + data["table_name"] + "..." + C.ENDC)
        con = self.connection()
        if replace:
            cleaning_request = '''DELETE FROM ''' + data["table_name"] +''' WHERE 1=1;'''
            print(C.WARNING + "Cleaning table " + data["table_name"] + C.ENDC)
            self.execute_query(cleaning_request)
            print(C.OKGREEN + "[OK] Cleaning Done" + C.ENDC)

        boolean = True
        index = 0
        total_rows = len(data["rows"])
        total_nb_batches = len(data["rows"]) // batch_size + 1
        while boolean:
            temp_row = []
            for i in range(batch_size):
                if not data["rows"]:
                    boolean = False
                    continue
                temp_row.append(data["rows"].pop())

            final_data = []
            for x in temp_row:
                for y in x:
                    final_data.append(y)

            if final_data:
                try:
                    temp_string = ','.join(map(lambda a: '(' + ','.join(map(lambda b: '%s', a)) + ')', tuple(temp_row))) % tuple(final_data)
                    inserting_request = '''INSERT INTO ''' + data["table_name"] + ''' (''' + ", ".join(
                        data["columns_name"]) + ''') VALUES ''' + temp_string + ''';'''
                    self._execute_query_custom(inserting_request)
                except Exception as e:
                    raise e
            index = index + 1
            percent = round(index * 100 / total_nb_batches, 2)
            if percent < 100:
                print("\r   %s / %s (%s %%)" % (str(index), total_nb_batches, str(percent)), end='\r')
            else:
                print("\r   %s / %s (%s %%)" % (str(index), total_nb_batches, str(percent)))

        print(C.HEADER + str(total_rows) + ' rows sent to BigQuery table ' + data["table_name"] + C.ENDC)
        print(C.OKGREEN + "[OK] Sent to bigquery" + C.ENDC)
        return 0

    def _send_data_custom(self,
                          data,
                          replace=True,
                          batch_size=1000,
                          other_table_to_update=None
                          ):
        """
        data = {
            "table_name" 	: 'name_of_the_redshift_schema' + '.' + 'name_of_the_redshift_table' #Must already exist,
            "columns_name" 	: [first_column_name,second_column_name,...,last_column_name],
            "rows"		: [[first_raw_value,second_raw_value,...,last_raw_value],...]
        }
        """
        data_copy = copy.deepcopy(data)
        try:
            self._send(data, replace=replace, batch_size=batch_size)
        except Exception as e:
            if " was not found " in str(e).lower() and " table " in str(e).lower():
                print("Destination table doesn't exist! Will be created")
                create_table(
                    self,
                    data=data_copy,
                    other_table_to_update=other_table_to_update
                )
                replace = False
            elif " is not present in table " in str(e).lower() and "column" in str(e).lower():
                create_columns(
                    self,
                    data=data_copy,
                    other_table_to_update=other_table_to_update
                )

            else:
                raise e

            self._send_data_custom(data_copy, replace=replace, batch_size=batch_size,
                                other_table_to_update=other_table_to_update)

    def clean(self, selecting_id, schema_prefix, table):
        print('trying to clean table %s.%s using %s' % (schema_prefix, table, selecting_id))
        cleaning_query = """
                DELETE FROM %(schema_name)s.%(table_name)s WHERE %(id)s IN (SELECT distinct %(id)s FROM %(schema_name)s.%(table_name)s_temp);
                INSERT INTO %(schema_name)s.%(table_name)s 
                SELECT * FROM %(schema_name)s.%(table_name)s_temp;
                DELETE FROM %(schema_name)s.%(table_name)s_temp;
                """ % {"table_name": table,
                       "schema_name": schema_prefix,
                       "id": selecting_id}
        self.execute_query(cleaning_query)
        print('cleaned')

    def get_max(self, schema, table, field, filter_clause=""):
        try:
            r = self.execute_query("SELECT max(%s) as max FROM %s.%s %s" % (field, schema, table, filter_clause))
            return r[0]["max"]
        except Exception as e:
            raise e

    def get_data_type(self, table_name, schema_name):
        query = """ SELECT column_name, data_type FROM %s.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='%s' """ \
                % (schema_name, schema_name)
        return self.execute_query(query=query)

    def create_view_from_columns(self, view_name, columns, schema_name, table_name):
        view_query = '''DROP VIEW IF EXISTS %s ;CREATE VIEW %s as (SELECT %s FROM %s.%s)''' \
                     % (view_name, view_name, columns, schema_name, table_name)
        self.execute_query(view_query)