import pandas as pd
from sqlite3 import connect

class Session:
    def __init__(self, db):
        self.con = connect(db)

    def __del__(self):
        self.con.close()

    def check_table(self, name):
        with self.con as con:
            cur = con.cursor()
            cur.execute('SELECT name FROM sqlite_master WHERE type="table" AND name="{}";'.format(name))
            exist = bool(len(cur.fetchall()))

        return exist

    def _init_table(self, name):
        with self.con as con:
            cur = con.cursor()
            cur.execute('CREATE TABLE IF NOT EXISTS "{}" (datetime TIMESTAMP PRIMARY KEY);'.format(name))

    def _drop_table(self, name):
        with self.con as con:
            cur = con.cursor()
            cur.execute('DROP TABLE IF EXISTS "{}";'.format(name))

    def fetch_df(self, query):
        with self.con as con:
            cur = con.cursor()
            cur.execute(query)
            result = cur.fetchall()
            columns = [desc[0] for desc in cur.description]

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

        self._init_table(name)

        for column in df.columns.values.tolist():
            _column = column.replace(' ', '_').lower()
            df.rename(columns={column: _column}, inplace=True)

        df.index.name = 'datetime'
        df.index = pd.to_datetime(df.index, utc=True).strftime('%Y-%m-%d %H:%M')
        df.reset_index(inplace=True)

        with self.con as con:
            cur = con.cursor()
            cur.execute('PRAGMA table_info("{}");'.format(name))
            table_info = cur.fetchall()

            for column in df.columns.values.tolist():
                exist = False
                for row in table_info:
                    if row[1] == column:
                        exist = True
                if not exist:
                    cur.execute('ALTER TABLE "{}" ADD COLUMN {} FLOAT;'.format(name, column))

            columns = ','.join(df.columns.values.tolist())
            values = df.itertuples(index=False, name=None)
            qmark = ','.join(['?' for i in range(len(df.columns.values.tolist()))])
            cur.executemany('INSERT OR IGNORE INTO "{}" ({}) VALUES ({});'.format(name, columns, qmark), values)
