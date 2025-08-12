import duckdb

DB_FILE = "banco_mtg.db"

def create_tables():
    """
    Lê o arquivo schema.sql e executa os comandos para criar
    as tabelas no banco de dados DuckDB.
    """
    print(f"Iniciando a criação do banco de dados em '{DB_FILE}'...")

    try:
        with open("schema.sql", "r") as f:
            sql_script = f.read()

        # Conecta ao banco (o arquivo será criado se não existir)
        con = duckdb.connect(database=DB_FILE)

        # Executa o script SQL para criar todas as tabelas
        con.execute(sql_script)

        # Fecha a conexão
        con.close()

        print("Banco de dados e tabelas criados com sucesso!")

    except FileNotFoundError:
        print("ERRO: Arquivo 'schema.sql' não encontrado. Certifique-se de que ele está na mesma pasta.")
    except Exception as e:
        print(f"Ocorreu um erro: {e}")

if __name__ == "__main__":
    create_tables()