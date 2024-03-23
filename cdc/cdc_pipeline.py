import psycopg2
import logging
from psycopg2 import sql
from dotenv import load_dotenv
import os

# Carregar as variáveis de ambiente a partir do arquivo .env
load_dotenv()

# Configurar o logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Conexão com o banco de dados PostgreSQL
try:
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    logger.info("Conexão com o banco de dados estabelecida com sucesso.")
except psycopg2.Error as e:
    logger.error(f"Erro ao conectar ao banco de dados: {e}")
    raise

# Abra um cursor para executar operações no banco de dados
cur = conn.cursor()

# Função para capturar as mudanças na tabela employees e inseri-las na tabela employees_cdc
def capture_changes():
    try:
        # Inicia uma transação
        conn.autocommit = False

        # Inserção inicial de 5 itens na tabela employees
        insert_data = [
            ('João', 'TI', 3000),
            ('Maria', 'RH', 2800),
            ('Pedro', 'Vendas', 3200),
            ('Ana', 'Financeiro', 3500),
            ('Carlos', 'Marketing', 3100)
        ]
        for data in insert_data:
            cur.execute(sql.SQL("INSERT INTO employees (name, department, salary) VALUES (%s, %s, %s)"), data)
            cur.execute(sql.SQL("INSERT INTO employees_cdc (change_type, id, name, department, salary) VALUES (%s, %s, %s, %s, %s)"), ('INSERT', cur.lastrowid, *data))
        
        # Mais 3 inserções na tabela employees
        more_inserts = [
            ('Lucas', 'Operações', 3000),
            ('Camila', 'TI', 3300),
            ('Paula', 'RH', 2900)
        ]
        for data in more_inserts:
            cur.execute(sql.SQL("INSERT INTO employees (name, department, salary) VALUES (%s, %s, %s)"), data)
            cur.execute(sql.SQL("INSERT INTO employees_cdc (change_type, id, name, department, salary) VALUES (%s, %s, %s, %s, %s)"), ('INSERT', cur.lastrowid, *data))

        # Inserção com atualização na tabela employees
        cur.execute("UPDATE employees SET salary = 3500 WHERE id = 1")
        cur.execute("INSERT INTO employees_cdc (change_type, id, name, department, salary) VALUES ('UPDATE', 1, 'João', 'TI', 3500)")

        # Commit da transação
        conn.commit()
        logger.info("Operações de CDC concluídas com sucesso.")

    except psycopg2.Error as e:
        # Rollback em caso de erro
        conn.rollback()
        logger.error(f"Erro durante as operações de CDC: {e}")

# Executar a função para capturar as mudanças
capture_changes()

# Fechar o cursor e a conexão com o banco de dados
cur.close()
conn.close()
