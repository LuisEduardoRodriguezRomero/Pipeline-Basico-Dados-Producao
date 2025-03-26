# Demonstração Prática 4 - Criação de Pipeline de Extração, Limpeza, Transformação e Enriquecimento de Dados
# Versão 1

# Imports
import csv
import psycopg2



# Dados da Conexão
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "dados_prod"
DB_USER = "postgres"
DB_PASSWORD = "postgres"

# Cria um novo banco de dados
try:

    conn = psycopg2.connect(
        host = DB_HOST,
        port = DB_PORT,
        database = DB_NAME,
        password = DB_PASSWORD,
        user = DB_USER

    )
    print("Conexão realizada com sucesso")

    cursor = conn.cursor()

# Cria uma tabela para armazenar os dados de produção de alimentos
    cursor.execute(
        '''CREATE TABLE producao (
                            produto TEXT,
                            quantidade INTEGER,
                            preco_medio REAL,
                            receita_total REAL
                        )'''
    )

# Grava e fecha a conexão
    cursor.close()
    conn.commit()
    conn.close()
    print("Tabela criada com sucesso")
except:
    print("Falha de conexão")

 #Abre o arquivo CSV com os dados de produção de alimentos
with open('./Dados/producao_alimentos.csv', 'r') as file:
    # Cria um leitor de CSV para ler o arquivo
    reader = csv.reader(file)

    # Pula a primeira linha, que contém os cabeçalhos das colunas
    next(reader)

    #Conecta ao banco de dados
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        password=DB_PASSWORD,
        user=DB_USER

    )

    cursor = conn.cursor()
    # Insere cada linha do arquivo na tabela do banco de dados
    for row in reader:
        produto = row[0],
        quantidade= row[1],
        preco_medio = float(row[2]),
        receita_total = float(row[3])

        cursor.execute(
            ''' INSERT INTO producao(produto,quantidade,preco_medio,receita_total)
            VALUES(%s,%s,%s,%s)
            ''',(produto,quantidade,preco_medio,receita_total)

        )
    conn.commit()
    print('Dados inseridos')

    cursor.close()

    conn.close()

print("Job Concluído com Sucesso!")








