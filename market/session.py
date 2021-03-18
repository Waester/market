import pandas as pd
from psycopg2 import connect
from psycopg2.extras import execute_values

class Session:
    def __init__(self, db):
        self.connection = connect(db)

    def check_table(self, name):
        with self.connection.cursor() as cursor:
            cursor.execute('SELECT * FROM information_schema.tables WHERE table_name=\'{}\';'.format(name))
            exist = bool(len(cursor.fetchall()))

        return exist

    def _init_table(self, name):
        with self.connection.cursor() as cursor:
            cursor.execute('CREATE TABLE "{}" (datetime TIMESTAMP PRIMARY KEY);'.format(name))

    def _drop_table(self, name):
        with self.connection.cursor() as cursor:
            cursor.execute('DROP TABLE "{}";'.format(name))

    def fetch_df(self, query):
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

        df = pd.DataFrame(result, columns=columns).set_index('datetime')
        df.index = pd.to_datetime(df.index, utc=True)
        return df

    def push_df(self, **kwargs):
        _dict = dict(**kwargs)
        df = _dict.get('dataframe')
        name = _dict.get('name')
        replace = _dict.get('replace', False)

        if len(df.index) < 1:
            return

        if replace:
            self._drop_table(name)

        if not self.check_table(name):
            self._init_table(name)

        for column in df.columns.values.tolist():
            _column = column.replace(' ', '_').lower()
            df.rename(columns={column: _column}, inplace=True)

        df.index.name = 'datetime'
        df.index = pd.to_datetime(df.index, utc=True).strftime('%Y-%m-%d %H:%M:%S')
        df.reset_index(inplace=True)

        with self.connection.cursor() as cursor:
            for column in df.columns.values.tolist():
                cursor.execute('ALTER TABLE "{}" ADD COLUMN IF NOT EXISTS {} FLOAT;'.format(name, column))

            columns = ','.join(df.columns.values.tolist())
            values = df.itertuples(index=False, name=None)
            execute_values(cursor, 'INSERT INTO "{}" ({}) VALUES %s ON CONFLICT DO NOTHING;'.format(name, columns), values)

        self.connection.commit()
