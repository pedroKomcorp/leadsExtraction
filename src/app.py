import json
import sqlite3
import requests


def resgatarChaveApi():
    with open('../users.json', 'r') as users_file:
        users = json.load(users_file)

    for user in users:
        if user.get('status') == 'active':
            print(f"Chave API ativa encontrada: {user.get('chave_api')}")
            return user.get('chave_api')
    print("Nenhuma chave API ativa encontrada.")
    return None


def alterarValidadeChaveApi(chave_api, todas_inativas=False):
    with open('../users.json', 'r') as users_file:
        users = json.load(users_file)

    for user in users:
        if user.get('chave_api') == chave_api:
            user['status'] = 'inactive'

    if todas_inativas:
        for user in users:
            user['status'] = 'active'

    with open('../users.json', 'w') as users_file:
        json.dump(users, users_file, indent=4)


def resgatarInfoCnpj(chave_api):
    try:
        conn = sqlite3.connect('leads.db')
        cur = conn.cursor()
        cur.execute('''SELECT ID, CNPJ FROM clientes WHERE CONFERENCIA = 0 LIMIT 1;''')
        result = cur.fetchone()
        if result:
            id, cnpj = result
            headers = {
                'Authorization': f'{chave_api}'
            }
            response = requests.get(f'https://api.cnpja.com/office/{cnpj}', headers=headers)
            return response, id
        else:
            return None, None
    except Exception as e:
        print('Erro ao resgatar informações do CNPJ \n Erro: ', e)
        return None, None


def criar_tabela_e_inserir_dados(dados):
    try:
        conn = sqlite3.connect('leads.db')
        cur = conn.cursor()

        # Criar a tabela se ela não existir
        cur.execute('''CREATE TABLE IF NOT EXISTS empresa (
                        updated TEXT,
                        taxId TEXT,
                        company_id INTEGER,
                        company_name TEXT,
                        equity TEXT,
                        nature_id INTEGER,
                        nature_text TEXT,
                        size_id INTEGER,
                        size_acronym TEXT,
                        size_text TEXT,
                        members TEXT,
                        alias TEXT,
                        founded TEXT,
                        head INTEGER,
                        status_id INTEGER,
                        status_text TEXT,
                        municipality INTEGER,
                        street TEXT,
                        number TEXT,
                        details TEXT,
                        district TEXT,
                        city TEXT,
                        state TEXT,
                        zip TEXT,
                        country_id INTEGER,
                        country_name TEXT,
                        phones TEXT,
                        emails TEXT,
                        mainActivity_id INTEGER,
                        mainActivity_text TEXT,
                        sideActivities TEXT
                    );''')

        # Preparar os dados para inserção
        updated = dados['updated']
        taxId = dados['taxId']
        company_id = dados['company']['id']
        company_name = dados['company']['name']
        equity = dados['company'].get('equity')
        nature_id = dados['company']['nature']['id']
        nature_text = dados['company']['nature']['text']
        size_id = dados['company']['size']['id']
        size_acronym = dados['company']['size']['acronym']
        size_text = dados['company']['size']['text']
        members = json.dumps(dados['company']['members'])
        alias = dados['alias']
        founded = dados['founded']
        head = int(dados['head'])
        status_id = dados['status']['id']
        status_text = dados['status']['text']
        municipality = dados['address']['municipality']
        street = dados['address']['street']
        number = dados['address']['number']
        details = dados['address']['details']
        district = dados['address']['district']
        city = dados['address']['city']
        state = dados['address']['state']
        zip_code = dados['address']['zip']
        country_id = dados['address']['country']['id']
        country_name = dados['address']['country']['name']
        phones = json.dumps(dados['phones'])
        emails = json.dumps(dados['emails'])
        mainActivity_id = dados['mainActivity']['id']
        mainActivity_text = dados['mainActivity']['text']
        sideActivities = json.dumps(dados['sideActivities'])

        # Inserir os dados na tabela
        cur.execute('''INSERT INTO empresa (
                        updated, taxId, company_id, company_name, equity, nature_id, nature_text, size_id, size_acronym, size_text, members, alias, founded, head, status_id, status_text, municipality, street, number, details, district, city, state, zip, country_id, country_name, phones, emails, mainActivity_id, mainActivity_text, sideActivities
                      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (updated, taxId, company_id, company_name, equity, nature_id, nature_text, size_id, size_acronym,
                     size_text, members, alias, founded, head, status_id, status_text, municipality, street, number,
                     details, district, city, state, zip_code, country_id, country_name, phones, emails,
                     mainActivity_id, mainActivity_text, sideActivities))

        conn.commit()
        conn.close()

        print("Dados inseridos com sucesso.")

    except Exception as e:
        print("Erro ao criar tabela ou inserir dados \nErro: ", e)


def salvaInfoCnpj(id, res_dados):
    try:
        criar_tabela_e_inserir_dados(res_dados)
        conn = sqlite3.connect('leads.db')
        cur = conn.cursor()
        cur.execute('''UPDATE clientes SET CONFERENCIA = 1 WHERE ID = ?;''', (id,))
        conn.commit()
        conn.close()

    except Exception as e:
        print('Erro ao salvar informações do CNPJ \n Erro: ', e)


def main():
    while True:
        chave_api = resgatarChaveApi()
        if not chave_api:
            print("Nenhuma chave API ativa encontrada.")
            break

        res, id = resgatarInfoCnpj(chave_api)
        if res is None or id is None:
            print("Todos os clientes com CONFERENCIA = 0 foram processados.")
            break

        print(res.json())

        if res.status_code == 200:
            print('Requisição bem-sucedida, salvando informações...')
            salvaInfoCnpj(id, res.json())
            continue
        elif res.status_code == 400:
            print('Erro na requisição')
            break
        elif res.status_code == 401:
            print('Chave de API inválida')
            alterarValidadeChaveApi(chave_api)
            continue
        elif res.status_code == 404:
            print('CNPJ não encontrado')
            conn = sqlite3.connect('leads.db')
            cur = conn.cursor()
            cur.execute('''DELETE FROM clientes WHERE ID = ?;''', (id,))
            conn.commit()
            conn.close()
            continue
        elif res.status_code == 429:
            print('Limite de requisições atingido. Alterando a chave da API...')
            alterarValidadeChaveApi(chave_api)
            chave_api = resgatarChaveApi()
            if not chave_api:
                print("Todas as chaves estão inativas. Reativando todas as chaves.")
                alterarValidadeChaveApi(None, todas_inativas=True)
            continue
        elif res.status_code == 503:
            print('Erro interno no servidor')
            break


if __name__ == "__main__":
    main()
