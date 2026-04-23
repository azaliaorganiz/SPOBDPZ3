import mysql.connector
import logging

logging.basicConfig(filename = 'pz.log', filemode = 'w', level=logging.INFO)


class SQLTable:
    def __init__(self, db_config, table_name):
        self.db_config = db_config
        self.table_name = table_name
        self.connection = None
        self.cursor = None
        self.columns = []
        self.connect()

    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            if self.connection.is_connected():
                self.cursor = self.connection.cursor(dictionary = True)
                logging.info("Подключение к базе данных MySQL успешно установлено")
                self._update_column_names()
                return self.connection
        except Exception as e:
            logging.error(f"Ошибка подключения к базе данных: {e}")
            raise e

    def disconnect(self):
        """Закрыть соединение"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            logging.info("Соединение закрыто")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def _update_column_names(self):
        try:
            self.cursor.execute(f"SHOW COLUMNS FROM {self.table_name}")
            self.columns = [row['Field'] for row in self.cursor.fetchall()]
        except Exception as e:
            logging.error(f"Ошибка при получении списка колонок: {e}")

    def _check_table_exists(self):
        self.cursor.execute("SHOW TABLES LIKE %s", (self.table_name,))
        return self.cursor.fetchone() is not None

    def _find_primary_key(self):
        self.cursor.execute(f"SHOW KEYS FROM {self.table_name} WHERE Key_name = 'PRIMARY'")
        result = self.cursor.fetchone()
        return result['Column_name'] if result else None

    def _log(self, query, params=None):
        logging.info(f"Query: {query} | Params: {params}")

    # CREATE
    def create_table(self, name, columns):
        """Создать таблицу. columns = {'name': 'VARCHAR(100)', 'age': 'INT'}"""
        cols = ', '.join([f"{k} {v}" for k, v in columns.items()])
        query = f"CREATE TABLE IF NOT EXISTS {name} (id INT AUTO_INCREMENT PRIMARY KEY, {cols})"
        self._log(query, ())
        self.cursor.execute(query)
        self.connection.commit()
        self.table_name = name
        self._update_column_names()

    def insert(self, table, data):
        """Вставить одну запись. Возвращает ID."""
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        self._log(query, list(data.values()))
        self.cursor.execute(query, list(data.values()))
        self.connection.commit()
        return self.cursor.lastrowid

    def insert_many(self, table, rows):
        """Вставить несколько записей. Возвращает количество."""
        if not rows:
            return 0
        columns = ', '.join(rows[0].keys())
        placeholders = ', '.join(['%s'] * len(rows[0]))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        values = [list(row.values()) for row in rows]
        self._log(query, values)
        self.cursor.executemany(query, values)
        self.connection.commit()
        return self.cursor.rowcount

    # READ
    def select(self, table, columns='*', where=None, order_by=None, limit=None):
        """Выбрать записи. Возвращает список словарей."""
        query = f"SELECT {columns} FROM {table}"
        params = []

        if where:
            conditions = ' AND '.join([f"{k} = %s" for k in where.keys()])
            query += f" WHERE {conditions}"
            params = list(where.values())

        if order_by:
            query += f" ORDER BY {order_by}"

        if limit:
            query += f" LIMIT {limit}"

        self._log(query, params)
        self.cursor.execute(query, params)
        return self.cursor.fetchall()

    def select_one(self, table, columns='*', where=None):
        """Выбрать одну запись."""
        results = self.select(table, columns, where, limit=1)
        return results[0] if results else None

    # === UPDATE ===
    def update(self, table, data, where):
        """Обновить записи. Возвращает количество."""
        set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
        where_clause = ' AND '.join([f"{k} = %s" for k in where.keys()])
        query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        self._log(query, list(data.values()) + list(where.values()))
        self.cursor.execute(query, list(data.values()) + list(where.values()))
        self.connection.commit()
        return self.cursor.rowcount

    # === DELETE ===
    def delete(self, table, where):
        """Удалить записи. Возвращает количество."""
        where_clause = ' AND '.join([f"{k} = %s" for k in where.keys()])
        query = f"DELETE FROM {table} WHERE {where_clause}"
        self._log(query, list(where.values()))
        self.cursor.execute(query, list(where.values()))
        self.connection.commit()
        return self.cursor.rowcount

#ПРИМЕР ПОЛЬЗОВАНИЯ
if __name__ == '__main__':
    db_config = {
        'user': 'j30084097_13418',
        'password': 'pPS090207/()',
        'host': 'srv221-h-st.jino.ru',
        'database': 'j30084097_13418'
    }

    with SQLTable(db_config, 'students') as db:
        # Создание таблицы
        db.create_table('students', {
            'name': 'VARCHAR(100)',
            'age': 'INT',
            'grade': 'VARCHAR(10)'
        })

        # Вставка одной записи
        new_id = db.insert('students', {'name': 'Анна', 'age': 20, 'grade': 'A'})
        print(f"Добавлен студент с ID: {new_id}")

        # Вставка нескольких записей
        rows = [
            {'name': 'Иван', 'age': 21, 'grade': 'B'},
            {'name': 'Мария', 'age': 19, 'grade': 'A'},
            {'name': 'Петр', 'age': 22, 'grade': 'C'}
        ]
        db.insert_many('students', rows)

        # Получение всех записей
        all_students = db.select('students')
        print(f"\nВсе студенты ({len(all_students)}):")
        for student in all_students:
            print(f"ID: {student['id']}, {student['name']}, {student['age']} лет, оценка {student['grade']}")

        # Получение записей с условием
        a_students = db.select('students', where={'grade': 'A'})
        print(f"\nСтуденты с оценкой A:")
        for student in a_students:
            print(f"{student['name']} - {student['age']} лет")

        # Получение одной записи
        student = db.select_one('students', where={'id': new_id})
        print(f"\nСтудент с ID {new_id}: {student}")

        # Обновление записи
        db.update('students', {'grade': 'A+'}, {'id': new_id})
        print(f"\nОбновлена оценка для студента с ID {new_id}")

        # Подсчет записей
        db.cursor.execute("SELECT COUNT(*) as count FROM students")
        total = db.cursor.fetchone()['count']
        print(f"\nВсего студентов в таблице: {total}")

        # Удаление записи (закомментировано)
        # db.delete('students', {'id': new_id})
        # print(f"Удален студент с ID {new_id}")
