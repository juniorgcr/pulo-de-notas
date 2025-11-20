# 📊 Verificador de Notas Fiscais Faltantes no Protheus

Este projeto tem como objetivo identificar notas fiscais que foram emitidas e registradas no banco MySQL, mas que não aparecem no sistema Protheus (SQL Server). Ele compara os dados entre os dois sistemas e gera um relatório com as notas que estão faltando.

## ⚙️ O que o script faz

- Conecta simultaneamente aos bancos MySQL e SQL Server
- Solicita ao usuário um intervalo de datas para consulta
- Executa consultas SQL específicas em cada banco
- Padroniza os dados para garantir uma comparação precisa
- Identifica notas fiscais presentes no MySQL e ausentes no Protheus
- Gera um arquivo Excel com as notas faltantes

## 🧪 Tecnologias utilizadas

- Python 3
- pymysql
- pyodbc
- pandas
- python-dotenv
- pyinstaller (opcional, para gerar executável)

## 📁 Estrutura esperada do arquivo `.env`

Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:

```env
# MySQL
MYSQL_HOST=localhost
MYSQL_USER=usuario
MYSQL_PASSWORD=senha
MYSQL_DATABASE=nome_do_banco
MYSQL_PORT=3306

# SQL Server
MSSQL_DRIVER=ODBC Driver 17 for SQL Server
MSSQL_SERVER=servidor_sql
MSSQL_PORT=1433
MSSQL_DATABASE=nome_do_banco
MSSQL_USER=usuario
MSSQL_PASSWORD=senha
MSSQL_TRUST_CERTIFICATE=Yes
MSSQL_ENCRYPT=Yes
MSSQL_APP=Python
MSSQL_APPLICATION_INTENT=ReadOnly

# Diretório de saída
DIRETORIO_SAIDA=C:\temp
```

## ▶️ Como executar

1. Instale as dependências:

   ```bash
   pip install -r requirements.txt
   ```

2. Execute o script:

   ```bash
   python nota_faltante_protheus.py
   ```

3. Informe a data inicial e final no formato `YYYY-MM-DD` quando solicitado.

4. O resultado será exibido no terminal e salvo como `notas_pendentes.xlsx` no diretório definido.

## 📦 Como gerar um executável (opcional)

Se quiser transformar o script em um executável:

```bash
pip install pyinstaller
pyinstaller --onefile --name notas_pendentes --add-data "c:\scripts_python\pulo_de_notas\.env;." "c:\scripts_python\pulo_de_notas\nota faltante protheus.py"
```

> Antes de gerar novamente, exclua as pastas `dist` e `build`.

## 📌 Observações importantes

- A comparação é feita com base no número da nota fiscal (NFE).
- Os dados são apenas lidos dos bancos, sem qualquer alteração.
- O script ajusta o formato da loja no MySQL para corresponder ao padrão do Protheus.
- O relatório gerado facilita a verificação de inconsistências entre os sistemas.

---
