#!/usr/bin/env python
# -*- coding:utf-8 -*-

class DBUtils:
    """
    数据库工具类
    """

    """:param
    db:     数据库连接:  db = pymysql.connect(host='192.168.1.1', user='root', password='1234', port=3306, db='database_name')
    cursor: 数据库游标:  cursor = db.cursor()
    data:   需写入数据:  Dataframe
    table:  写入表名    
    """

    def __init__(self, db, cursor, data, table):
        self.db = db
        self.cursor = cursor
        self.data = data
        self.table = table

    # 按主键去重追加更新
    def insert_data(self):
        keys = ', '.join('`' + self.data.keys() + '`')
        values = ', '.join(['%s'] * len(self.data.columns))
        # 根据表的唯一主键去重追加更新
        sql = 'INSERT INTO {table}({keys}) VALUES ({values}) ON DUPLICATE KEY UPDATE'.format(table=self.table,
                                                                                             keys=keys,
                                                                                             values=values)
        update = ','.join(["`{key}` = %s".format(key=key) for key in self.data])
        sql += update

        for i in range(len(self.data)):
            try:
                self.cursor.execute(sql, tuple(self.data.loc[i]) * 2)
                print('正在写入第%d条数据' % (i + 1))
                self.db.commit()
            except Exception as e:
                print("数据写入失败,原因为:" + e)
                self.db.rollback()

        self.cursor.close()
        self.db.close()
        print('数据已全部写入完成!')
