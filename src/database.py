import pandas as pd
import sqlite3

# Carrega o DataFrame de códigos
cnpjs = pd.read_excel('../0150.xlsx', header=0, engine="openpyxl")

# Conecta ao banco de dados SQLite (ou cria um novo arquivo .db se não existir)
conn = sqlite3.connect('leads.db')
cursor = conn.cursor()

# Cria a tabela no banco de dados SQLite
cursor.execute(
    '''
CREATE TABLE IF NOT EXISTS clientes (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    NOME TEXT,
    CNPJ TEXT,
    INSCRICAO_ESTADUAL TEXT,
    CONFERENCIA BOOLEAN
)
''')

# Insere os dados do DataFrame no banco de dados SQLite
cnpjs.to_sql('clientes', conn, if_exists='append', index=False)

# Confirma as mudanças e fecha a conexão
conn.commit()
conn.close()
