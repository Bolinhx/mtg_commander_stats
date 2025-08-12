import duckdb

DB_FILE = "banco_mtg.db"

def populate_static_dims():
    """Insere os dados em dimensões estáticas como Métodos de Eliminação."""
    print("Populando dimensões estáticas...")

    metodos = [
        (1, 'C', 'Dano de Combate'),
        (2, 'D', 'Dano de Comandante'),
        (3, 'N', 'Dano Não-Combate / Dreno de Vida'),
        (4, 'O', 'Outros (Mill, Infect, Wincon, etc)')
    ]

    try:
        con = duckdb.connect(database=DB_FILE)
        con.executemany("INSERT OR IGNORE INTO Dim_Metodos_Eliminacao (ID_METODO, CODIGO_METODO, DESCRICAO_METODO) VALUES (?, ?, ?)", metodos)
        con.close()
        print("Tabela Dim_Metodos_Eliminacao populada com sucesso!")
    except Exception as e:
        print(f"ERRO ao popular Dim_Metodos_Eliminacao: {e}")

if __name__ == "__main__":
    populate_static_dims()