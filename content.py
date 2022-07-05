from canvas_data.api import CanvasDataAPI
import pandas as pd
from config import *
pd.options.mode.chained_assignment = None


class Get_schema:
    def __init__(self, table_name, dump_id=None):

        if dump_id is None:
            self.dump_id = 'latest'
        else:
            self.dump_id = dump_id
        self.api_key = api_key
        self.api_secret = api_secret
        self.table_name = table_name
        self.cd = CanvasDataAPI(api_key=self.api_secret, api_secret=self.api_secret, download_chunk_size=1024 * 1024)
        self.dump_latest = self.cd.get_schema(self.dump_id, key_on_tablenames=True)
        self.get_schema = pd.json_normalize(self.dump_latest[self.table_name]['columns'])

    def check_db(self):
        check_query = f"""""SELECT EXISTS (
                SELECT FROM 
                    information_schema.tables 
                WHERE table_name = '{self.table_name}' );"""""
        return check_query  # ADD Execute query function

    def create_table(self, schema_name):
        sql = []
        if 'length' in self.get_schema.columns:
            df = self.get_schema[['name', 'type', 'length']]
            df.length.iloc[:].fillna(0, inplace=True)
            df.length.iloc[:] = df.length.astype(int)
        else:
            df = self.get_schema[['name', 'type']]

        for i, v in enumerate(df.values.tolist()):
            if i == 0:
                sql.append(f"CREATE TABLE IF NOT EXISTS {schema_name}.{self.table_name} (")
                if len(v) == 3 and v[2] == 0:
                    sql.extend(v[:-1])
                    sql.append(",")
                else:
                    sql.extend(v)
                    sql.append(",")
            elif i == len(df.values.tolist()) - 1:
                if len(v) == 3 and v[2] == 0:
                    sql.extend(v[:-1])
                    sql.append(");")
                else:
                    sql.extend(v)
                    sql.append(");")
            else:
                if len(v) == 3 and v[2] == 0:
                    sql.extend(v[:-1])
                    sql.append(",")
                else:
                    sql.extend(v)
                    sql.append(",")
        return print(' '.join(map(str, sql)).upper())

    def get_data(self):
        local_data_filename = self.cd.get_data_for_table(table_name=self.table_name, dump_id=self.dump_id)
        try:
            data = pd.read_csv(local_data_filename, header=None, sep="\t")
            data.columns = self.get_schema['name'].to_list()
            return data
        except Exception as e:
            if f'{e}' == 'No columns to parse from file':
                return []
            else:
                data = pd.read_csv(local_data_filename, header=None, sep="\t")
                data.columns = self.get_schema['name'].to_list()
                return data

    def insert_table(self):
        df = self.get_schema[['name']]
        sql = []
        for i, v in enumerate(df.values.tolist()):
            if i == 0:
                sql.append(f"""""INSERT INTO {self.table_name.upper()} (""""")
                sql.extend(v)
                sql.append(",")
            elif i == len(df.values.tolist()) - 1:
                sql.extend(v)
                sql.append(") VALUES")
            else:
                sql.extend(v)
                sql.append(",")

        for i1, v1 in enumerate(self.get_data().values.tolist()):
            value = []
            for i2, v2 in enumerate(v1):
                if i2 == 0:
                    value.append("(")
                item = f"""'{v2}'"""
                if v2 == "\\N":
                    item = """''"""
                value.append(item)
                if i2 != len(v1) - 1:
                    value.append(",")
                else:
                    value.append(")")
            s = ' '.join(map(str, value))
            sql.append(s)
            if i1 != len(self.get_data().values.tolist()) - 1:
                sql.append(",")
            else:
                sql.append(";")
        return ' '.join(map(str, sql))
