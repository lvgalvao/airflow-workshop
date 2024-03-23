import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os

# Carregar as variáveis de ambiente a partir do arquivo .env
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

# Função para capturar as mudanças na tabela employees e inseri-las na tabela employees_cdc
def capture_changes(name, department, salary):
    try:
        # Inicia uma transação
        conn.autocommit = False
        
        # Inserir na tabela employees
        cur.execute(sql.SQL("INSERT INTO employees (name, department, salary) VALUES (%s, %s, %s)"), (name, department, salary))
        
        # Capturar o último ID inserido na tabela employees
        cur.execute("SELECT LASTVAL()")
        employee_id = cur.fetchone()[0]

        # Inserir na tabela employees_cdc
        cur.execute(sql.SQL("INSERT INTO employees_cdc (change_type, id, name, department, salary) VALUES (%s, %s, %s, %s, %s)"), ('INSERT', employee_id, name, department, salary))
        
        # Commit da transação
        conn.commit()

        print("Inserção realizada com sucesso!")
    
    except Exception as e:
        # Rollback em caso de erro
        conn.rollback()
        print("Erro durante a inserção:", e)

# Exemplo de inserção que requer a criação de um registro de tipo 2
# Suponha que o salário de um funcionário seja atualizado
def update_employee_salary(employee_id, new_salary):
    try:
        # Inicia uma transação
        conn.autocommit = False
        
        # Atualizar o salário na tabela employees
        cur.execute(sql.SQL("UPDATE employees SET salary = %s WHERE id = %s"), (new_salary, employee_id))
        
        # Inserir um novo registro na tabela employees_cdc (tipo 2)
        cur.execute(sql.SQL("INSERT INTO employees_cdc (change_type, id, name, department, salary) SELECT %s, id, name, department, %s FROM employees WHERE id = %s"), ('UPDATE', new_salary, employee_id))
        
        # Commit da transação
        conn.commit()

        print("Salário do funcionário atualizado com sucesso!")
    
    except Exception as e:
        # Rollback em caso de erro
        conn.rollback()
        print("Erro durante a atualização do salário:", e)

# Executar a função para capturar as mudanças (essa seria uma operação contínua ou agendada)
capture_changes('João', 'TI', 3000)  # Exemplo de inserção simples

# Exemplo de atualização de salário que requer a criação de um registro de tipo 2
update_employee_salary(1, 3500)  # Atualizar o salário do funcionário com ID 1 para 3500

# Fechar o cursor e a conexão com o banco de dados
cur.close()
conn.close()
