import sqlite3
import os

class NewsDb:
    def __init__(self, root):
        path = os.path.join(root, 'news.db')
        self.conn = sqlite3.connect(path)
        self.create_table()
        self.stmts = []
    def __del__(self): self.conn.close()
    def create_table(self):    
        cur = self.conn.cursor()
#        cur.execute(self.__create_table_sql())
        cur.executescript(self.__create_table_sql())
    def __create_table_sql(self):
        return '''
create table if not exists news(
  id         integer primary key,
  published  text,
  url        text,
  title      text,
  body       text  -- URL先から本文だけを抽出したプレーンテキスト
);
create index if not exists idx_news on 
  news(published desc, id desc, url, title);
create table if not exists sources(
  id       integer primary key,
  domain   text, -- URLのドメイン名
  name     text, -- 情報源名
  created  text  -- 登録日時（同一ドメイン名が複数あるとき新しいほうを表示する）
);
create index if not exists idx_sources on 
  sources(domain, created desc, id desc, name);
'''
    def __insert_sql(self): 
        return 'insert into news(published,url,title,body) values(?,?,?,?)'
    def append_insert_stmt(self, published, url, title, body):
        self.stmts.append((published, url, title, body))
#        self.stmts.append(
#            "insert into news(published,url,title,body) values("
#            + "'" + published + "',"
#            + "'" + url       + "',"
#            + "'" + title     + "',"
#            + "'" + body      + "'"
#            + ");");
    def insert(self):
        if 0 == len(self.stmts): return
        try:
            cur = self.conn.cursor()
            cur.executemany(self.__insert_sql(), self.stmts)
            self.conn.commit()
            self.stmts.clear()
        except: 
            import traceback
            traceback.print_exc()
            self.conn.rollback()
#        finally: self.stmts.clear()

