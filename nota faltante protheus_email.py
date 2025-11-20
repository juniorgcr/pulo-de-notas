import os
import pymysql
import pyodbc
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
import sys

# Carregamento robusto do .env: procura _MEIPASS (arquivos embutidos pelo PyInstaller),
# depois procura .env na pasta do executável e por fim no diretório do script / cwd.
potential_paths = []
if getattr(sys, "frozen", False):
    if hasattr(sys, "_MEIPASS"):
        potential_paths.append(os.path.join(sys._MEIPASS, ".env"))
    potential_paths.append(os.path.join(os.path.dirname(sys.executable), ".env"))
else:
    potential_paths.append(os.path.join(os.path.dirname(__file__), ".env"))
    potential_paths.append(os.path.join(os.getcwd(), ".env"))

dotenv_path = next((p for p in potential_paths if os.path.exists(p)), None)
if dotenv_path:
    load_dotenv(dotenv_path=dotenv_path)
else:
    # fallback: tenta carregar variáveis do ambiente caso já estejam definidas
    load_dotenv()

diretorio = os.getenv("DIRETORIO_SAIDA", r"C:\temp")

# Função auxiliar para conectar com tratamento de erro
def connect_mysql():
    try:
        return pymysql.connect(
            host=os.getenv("MYSQL_HOST"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE"),
            port=int(os.getenv("MYSQL_PORT", 3306)),
            connect_timeout=10
        )
    except Exception as e:
        print(f"Erro ao conectar MySQL: {e}")
        sys.exit(1)

def connect_sqlserver():
    try:
        return pyodbc.connect(
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
    except Exception as e:
        print(f"Erro ao conectar SQL Server: {e}")
        sys.exit(1)

# Cria conexões
mysql_con = connect_mysql()
sqlserver_con = connect_sqlserver()

def get_yesterday_date():
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')

data_ini = get_yesterday_date()
data_fim = data_ini

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

# Lê os dados (tratando possíveis DataFrames vazios)
try:
    df_mysql = pd.read_sql(query_mysql, mysql_con, params=[data_ini, data_fim])
except Exception as e:
    print(f"Erro ao executar query MySQL: {e}")
    sys.exit(1)

try:
    df_sqlserver = pd.read_sql(query_sqlserver, sqlserver_con, params=[data_ini_sql, data_fim_sql])
except Exception as e:
    print(f"Erro ao executar query SQL Server: {e}")
    sys.exit(1)

# Se DataFrames vazios, cria colunas esperadas para evitar KeyError
if df_mysql.empty:
    df_mysql = pd.DataFrame(columns=['numero_nfe', 'NroCupom', 'Pdv', 'nroloja', 'dthr_emit_nfe'])
if df_sqlserver.empty:
    df_sqlserver = pd.DataFrame(columns=['L1_DOC', 'L1_XXARIUS', 'L1_SERIE', 'L1_FILIAL', 'L1_EMISSAO'])

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

# Ajuste formato Loja para permitir comparação (conforme sua regra original)
# Exemplo: MySQL nroloja '1' -> '0101001' (se essa era a regra); ajuste conforme necessário.
# Aqui aplico a transformação que você usava antes: "0101" + nroloja zero-fill 3 -> total 7 chars
def format_loja_mysql(x):
    try:
        return f"0101{int(x):03d}"
    except Exception:
        return str(x).zfill(7)

df_mysql['Loja'] = df_mysql['Loja'].apply(format_loja_mysql)
df_sqlserver['Loja'] = df_sqlserver['Loja'].astype(str).str.zfill(7)

# Garante tipos
df_mysql['NFE'] = df_mysql['NFE'].astype(str)
df_sqlserver['NFE'] = df_sqlserver['NFE'].astype(str)
df_mysql['Loja'] = df_mysql['Loja'].astype(str)
df_sqlserver['Loja'] = df_sqlserver['Loja'].astype(str)
df_mysql['Serie'] = df_mysql['Serie'].astype(str)
df_sqlserver['Serie'] = df_sqlserver['Serie'].astype(str)

# Tenta converter Emissao para date; se falhar, mantém como string
for df, col in ((df_mysql, 'Emissao'), (df_sqlserver, 'Emissao')):
    try:
        df[col] = pd.to_datetime(df[col]).dt.date
    except Exception:
        # mantém valores originais ou NaT
        df[col] = df[col]

# Lógica para encontrar NFs que estão no MySQL mas não no SQL Server.
nfe_mysql_set = set(df_mysql['NFE'])
nfe_sqlserver_set = set(df_sqlserver['NFE'])
nfs_faltantes = sorted(list(nfe_mysql_set - nfe_sqlserver_set))

print(f"Encontradas {len(nfs_faltantes)} notas fiscais faltantes no sistema Protheus.")

# Filtra o df_mysql original para pegar as linhas correspondentes às NFs faltantes
pendentes = df_mysql[df_mysql['NFE'].isin(nfs_faltantes)].copy()

# Organiza as colunas para o arquivo final
colunas_finais = ['Loja', 'Serie', 'Emissao', 'NFE', 'Cupom']
# Garante que todas as colunas existam
for c in colunas_finais:
    if c not in pendentes.columns:
        pendentes[c] = ""

pendentes_finais = pendentes[colunas_finais]

# Exibe o resultado no prompt
print("\n--- Notas Fiscais Faltantes no Protheus, favor checar ---\n")
if not pendentes_finais.empty:
    print(pendentes_finais.to_string(index=False))
else:
    print(" --- SEM PULO DE NOTA PARA DATA INFORMADA ---")
print("\n---------------------------------\n")

# Salva resultado em Excel (apenas se houver dados)
os.makedirs(diretorio, exist_ok=True)
caminho_excel = os.path.join(diretorio, "notas_pendentes.xlsx")

if not pendentes_finais.empty:
    try:
        pendentes_finais.to_excel(caminho_excel, index=False)
        print(f"Arquivo com as notas faltantes salvo em: {caminho_excel}")
    except Exception as e:
        print(f"Erro ao salvar Excel: {e}")
        sys.exit(1)
else:
    # Se o arquivo existir, apaga para não manter dados antigos
    if os.path.exists(caminho_excel):
        try:
            os.remove(caminho_excel)
        except Exception:
            pass
    print("Nenhuma nota fiscal pendente encontrada. O arquivo de pendências não foi gerado/foi removido.")

print(f"Total de notas faltantes encontradas: {len(pendentes_finais)}")
print("\n---------------------------------\n")
print("Verifique o arquivo para mais detalhes.")
#quando for necessário atualizar o executável é necessário apagar a pasta dist e build e depois rodar o comando abaixo#
# -- pip install pyinstaller
# -- pyinstaller --onefile --name notas_pendentes --add-data "c:\scripts_python\pulo_de_notas\.env;. " "c:\scripts_python\pulo_de_notas\nota faltante protheus.py"