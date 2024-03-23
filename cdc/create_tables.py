import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os

load_dotenv()

# Conexão com o banco de dados PostgreSQL
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)

# Abra um cursor para executar operações no banco de dados
cur = conn.cursor()

# Leia o conteúdo do arquivo SQL
with open('create_tables.sql', 'r') as file:
    sql_query = file.read()

# Execute as consultas SQL
cur.execute(sql_query)

# Commit as alterações
conn.commit()

# Fechar o cursor e a conexão com o banco de dados
cur.close()
conn.close()