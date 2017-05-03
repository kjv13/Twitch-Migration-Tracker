from lib.db_connect import NoSQLConnection


def main():
    con = NoSQLConnection()
    con.recreate_db()


if __name__ == '__main__':
    main()
