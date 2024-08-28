import json
import pandas as pd
import sqlite3

# Conectar ao banco de dados SQLite
conn = sqlite3.connect('../leads.db')
cur = conn.cursor()

# Executar a consulta para obter todos os dados JSON
cur.execute('SELECT members, taxId FROM empresa')
rows = cur.fetchall()

# Lista para armazenar as linhas do DataFrame
data_rows = []

# Iterar sobre cada linha retornada
for row in rows:
    json_data = row[0]

    # Verificar se json_data não é None e é uma string
    if json_data and isinstance(json_data, str):
        members = json.loads(json_data)

        # Iterar sobre cada item no JSON
        for item in members:
            # Verificar se a função está em 'agent'
            if 'agent' in item and 'person' in item['agent']:
                name = item['agent']['person']['name']
            else:
                name = item['person']['name']

            # Verificar se a função está em 'role'
            if 'role' in item and 'text' in item['role']:
                role = item['role']['text']
            else:
                role = 'Função não encontrada'

            # Adicionar a linha com nome e função à lista
            data_rows.append({'Nome': name, 'Função': role})

# Criar um DataFrame a partir das linhas
df = pd.DataFrame(data_rows)

# Salvar o DataFrame em um arquivo Excel
df.to_excel('nomes_funcoes.xlsx', index=False)

print('Arquivo Excel criado com sucesso!')
