import os
import pymysql
import pyodbc
import pandas as pd
from dotenv import load_dotenv

# Carrega variáveis do .env
dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(dotenv_path=dotenv_path)

diretorio = os.getenv("DIRETORIO_SAIDA", r"C:\temp")

# Conexão MySQL
mysql_con = pymysql.connect(
    host=os.getenv("MYSQL_HOST"),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD"),
    database=os.getenv("MYSQL_DATABASE"),
    port=int(os.getenv("MYSQL_PORT", 3306))
)

# Conexão SQL Server
sqlserver_con = pyodbc.connect(
    f"DRIVER={{{os.getenv('MSSQL_DRIVER')}}};"
    f"SERVER={os.getenv('MSSQL_SERVER')},{os.getenv('MSSQL_PORT')};"
    f"DATABASE={os.getenv('MSSQL_DATABASE')};"
    f"UID={os.getenv('MSSQL_USER')};"
    f"PWD={os.getenv('MSSQL_PASSWORD')};"
    f"TrustServerCertificate={os.getenv('MSSQL_TRUST_CERTIFICATE', 'Yes')};"
    f"Encrypt={os.getenv('MSSQL_ENCRYPT', 'Yes')};"
    f"APP={os.getenv('MSSQL_APP', 'Python')};"
    f"ApplicationIntent={os.getenv('MSSQL_APPLICATION_INTENT', 'ReadOnly')};"
)

def input_datas():
    print("Informe a data inicial para a consulta dos dados\n (YYYY-MM-DD):")
    data_ini = input().strip()
    print("Informe a data final para consulta dos dados\n (YYYY-MM-DD):")
    data_fim = input().strip()
    return data_ini, data_fim

data_ini, data_fim = input_datas()

# Converte para o formato YYYYMMDD para SQL Server
data_ini_sql = data_ini.replace("-", "")
data_fim_sql = data_fim.replace("-", "")

# Query MySQL
query_mysql = """
SELECT numero_nfe, NroCupom, Pdv, nroloja, dthr_emit_nfe
FROM nfce
WHERE DATE(dthr_emit_nfe) BETWEEN %s AND %s
  AND numero_nfe IS NOT NULL
"""

# Query SQL Server
query_sqlserver = """
SELECT
    SF2.F2_DOC AS L1_DOC,
    ISNULL(SL1.L1_XXARIUS, '') AS L1_XXARIUS,
    SF2.F2_SERIE AS L1_SERIE,
    SF2.F2_FILIAL AS L1_FILIAL,
    SF2.F2_EMISSAO AS L1_EMISSAO
FROM
    SF2010 AS SF2
LEFT JOIN
    SL1010 AS SL1 ON SF2.F2_DOC = SL1.L1_DOC AND SF2.F2_SERIE = SL1.L1_SERIE AND SF2.F2_FILIAL = SL1.L1_FILIAL
WHERE
    SF2.F2_EMISSAO BETWEEN ? AND ?
    AND LEN(SF2.F2_SERIE) = 3
"""

# Busca dados MySQL (usa data YYYY-MM-DD)
df_mysql = pd.read_sql(query_mysql, mysql_con, params=[data_ini, data_fim])
# Busca dados SQL Server (usa data YYYYMMDD)
df_sqlserver = pd.read_sql(query_sqlserver, sqlserver_con, params=[data_ini_sql, data_fim_sql])

# Normaliza tipos e nomes para comparação
df_mysql['numero_nfe'] = df_mysql['numero_nfe'].astype(str).str.zfill(9)
df_sqlserver['L1_DOC'] = df_sqlserver['L1_DOC'].astype(str).str.zfill(9)
df_mysql['Pdv'] = df_mysql['Pdv'].astype(str).str.zfill(3)
df_sqlserver['L1_SERIE'] = df_sqlserver['L1_SERIE'].astype(str).str.zfill(3)
df_mysql['nroloja'] = df_mysql['nroloja'].astype(str).str.zfill(2)
# NÃO AJUSTE df_sqlserver['L1_FILIAL'] aqui!

# Renomeia para facilitar merge
df_mysql = df_mysql.rename(columns={
    'numero_nfe': 'NFE',
    'NroCupom': 'Cupom',
    'Pdv': 'Serie',
    'nroloja': 'Loja',
    'dthr_emit_nfe': 'Emissao'
})
df_sqlserver = df_sqlserver.rename(columns={
    'L1_DOC': 'NFE',
    'L1_XXARIUS': 'Cupom',
    'L1_SERIE': 'Serie',
    'L1_FILIAL': 'Loja',
    'L1_EMISSAO': 'Emissao'
})

# Agora sim, ajuste Loja do MySQL para formato do SQL Server
df_mysql['Loja'] = df_mysql['Loja'].apply(lambda x: f"0101{int(x):03d}")
df_sqlserver['Loja'] = df_sqlserver['Loja'].astype(str).str.zfill(7)

# Garante que os campos de comparação são do mesmo tipo
df_mysql['NFE'] = df_mysql['NFE'].astype(str)
df_sqlserver['NFE'] = df_sqlserver['NFE'].astype(str)
df_mysql['Loja'] = df_mysql['Loja'].astype(str)
df_sqlserver['Loja'] = df_sqlserver['Loja'].astype(str)
df_mysql['Serie'] = df_mysql['Serie'].astype(str)
df_sqlserver['Serie'] = df_sqlserver['Serie'].astype(str)
df_mysql['Emissao'] = pd.to_datetime(df_mysql['Emissao']).dt.date
df_sqlserver['Emissao'] = pd.to_datetime(df_sqlserver['Emissao']).dt.date

# Lógica para encontrar NFs que estão no MySQL mas não no SQL Server.
# A comparação é feita apenas pelo número da NFE.
nfe_mysql_set = set(df_mysql['NFE'])
nfe_sqlserver_set = set(df_sqlserver['NFE'])

nfs_faltantes = sorted(list(nfe_mysql_set - nfe_sqlserver_set))

print(f"Encontradas {len(nfs_faltantes)} notas fiscais faltantes no sistema Protheus.")

# Filtra o df_mysql original para pegar as linhas correspondentes às NFs faltantes
pendentes = df_mysql[df_mysql['NFE'].isin(nfs_faltantes)].copy()

# Organiza as colunas para o arquivo final
colunas_finais = ['Loja', 'Serie', 'NFE', 'Cupom', 'Emissao']
pendentes_finais = pendentes[colunas_finais]

# Exibe o resultado no prompt
print("\n--- Notas Fiscais Faltantes no Protheus, favor checar ---\n")
if not pendentes_finais.empty:
    print(pendentes_finais.to_string())
else:
    print(" --- SEM PULO DE NOTA PARA DATA INFORMADA --- ")
print("\n---------------------------------\n")

# Salva resultado em Excel
os.makedirs(diretorio, exist_ok=True)
caminho_excel = os.path.join(diretorio, "notas_pendentes.xlsx")
pendentes_finais.to_excel(caminho_excel, index=False)


print(f"Total de notas faltantes encontradas: {len(pendentes_finais)}")
print("\n---------------------------------\n")
print(f"Arquivo com as notas faltantes salvo em: {caminho_excel}")
print("\n---------------------------------\n")
print("Caso tenha gerado pulos de notas, verifique o arquivo para mais detalhes.")

print("\n---------------------------------\n")


#quando for necessário atualizar o executável é necessário apagar a pasta dist e build e depois rodar o comando abaixo#
# -- pip install pyinstaller
# -- pyinstaller --onefile --name notas_pendentes --add-data "c:\scripts_python\pulo_de_notas\.env;." "c:\scripts_python\pulo_de_notas\nota faltante protheus.py"