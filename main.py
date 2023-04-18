from PyQt5.QtWidgets import *
from PyQt5.QtGui import *
from PyQt5.QtCore import Qt

import sys, psycopg2, os

class MyWidget(QWidget):
    def __init__(self):
        super().__init__()
        self.initGUI()
        self.DATABASE_URL = ''
    
    def initGUI(self):
        self.setWindowTitle('sql-operate-tool')
        self.resize(800, 400)
        self.setFixedSize(400, 600)
        # self.setGeometry(0, 0, 200, 150)

        font = QFont()
        font.setPointSize(15)
        # font.setBold(True)

        self.URL_label = QLabel(self)
        self.URL_label.move(100, 10)
        self.URL_label.setText("資料庫外部網址:")
        self.URL_label.setFont(font)

        self.URL = QLineEdit(self)
        self.URL.move(20, 40)
        self.URL.resize(350,30)

        self.implement = QPushButton('執行', self)
        self.implement.move(150, 150)
        self.implement.setFont(font)
        self.implement.clicked.connect(self.judge)

        self.function_label = QLabel(self)
        self.function_label.move(100, 80)
        self.function_label.setText("常用(快速)功能:")
        self.function_label.setFont(font)

        self.function = QComboBox(self)
        self.function.addItems(['查詢全部資料表', '建立資料表', '備份資料庫', '將檔案匯入到本地資料庫'])
        self.function.setFont(font)
        self.function.resize(300,30)
        self.function.move(50, 110)

    def judge(self):
        u = self.URL.text().replace(' ', '')
        if u != '':
            self.DATABASE_URL = self.URL.text()
        else:
            alert= QMessageBox(self)
            alert.information(self, 'error', 'please input database URL!')
            return
        if self.function.currentText() == '查詢全部資料表':
            self.SearchTables()
        elif self.function.currentText() == '建立資料表':
            self.CreateTables()
        elif self.function.currentText() == '備份資料庫':
            self.BackupDatabase()
        elif self.function.currentText() == '將檔案匯入到本地資料庫':
            self.InsertDatabase()

    def CreateTables(self):
        try:
            db = psycopg2.connect(self.DATABASE_URL, sslmode='require')
            cursor = db.cursor()
            cursor.execute(
            '''CREATE TABLE galgameTitle(
                TITLE VARCHAR(255) NOT NULL,
                TAG VARCHAR(255) NOT NULL,
                CREATE_TIME timestamp);''')
            cursor.execute(
            '''CREATE TABLE galgameArticle(
                TITLE VARCHAR(255) NOT NULL,
                COMPANY VARCHAR(255) NOT NULL,
                START_TIME VARCHAR(15) NOT NULL,
                END_TIME VARCHAR(15) NOT NULL,
                OP_URL VARCHAR(200) NOT NULL,
                TAG VARCHAR(50) NOT NULL,
                AUTHOR VARCHAR(20) NOT NULL,
                CONTENT TEXT NOT NULL,
                CREATE_TIME timestamp);''')
            cursor.execute(
            '''CREATE TABLE article(
                TITLE VARCHAR(255) NOT NULL,
                TAG VARCHAR(50) NOT NULL,
                AUTHOR VARCHAR(20) NOT NULL,
                CONTENT TEXT NOT NULL,
                CREATE_TIME timestamp);''')
            db.commit()
            cursor.close()
        except:
            alert= QMessageBox(self)
            alert.information(self, 'error', 'connect error or insert error !')

    def SearchTables(self):
            db = psycopg2.connect(self.DATABASE_URL, sslmode='require')
            cursor = db.cursor()
            cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
            db.commit()
            st = cursor.fetchall()
            alert= QMessageBox(self)
            alert.information(self, 'info', f'資料表陣列: {str(st)}')
            cursor.close()

    def BackupDatabase(self):
        try:
            db = psycopg2.connect(self.DATABASE_URL, sslmode='require')
            cursor = db.cursor()
            cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
            st = cursor.fetchall()
            os.makedirs('./backupDB', exist_ok=True)
            for s in st:
                cursor.execute(f'''SELECT * from {s[0]};''')
                rows = cursor.fetchall()
                with open(f'./backupDB/{s[0]}.txt', 'w', encoding='utf-8-sig') as f:
                    for row in rows:
                        f.write(str(row))
                        f.write('\n')
                    f.close()
                    # print('Backup:' + s[0] + '  ......')
                    alert= QMessageBox(self)
                    alert.information(self, 'hint', f'{str(s[0])} backup complete!')
            cursor.close()
        except:
            alert= QMessageBox(self)
            alert.information(self, 'error', 'Cannot backup database !')

    def InsertDatabase(self):
        print(self.URL.text())

if __name__ == '__main__':
    app = QApplication(sys.argv)
    w = MyWidget()
    w.show()
    sys.exit(app.exec_())