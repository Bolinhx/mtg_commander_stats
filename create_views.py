import duckdb

DB_FILE = "banco_mtg.db"

# Esta consulta SQL junta todas as nossas tabelas para criar uma visão completa
# de cada participação de jogador em cada partida.
CREATE_VIEW_SQL = """
CREATE OR REPLACE VIEW vw_desempenho_completo AS
SELECT
    p.DATA_PARTIDA,
    p.NUM_JOGADORES,
    j_vencedor.NOME_JOGADOR AS NOME_VENCEDOR,
    dp.POSICAO,
    dp.PONTUACAO,
    dj.NOME_JOGADOR,
    dc.NOME_COMANDANTE_EN,
    dc.IDENTIDADE_DE_COR,
    (dp.KILLS_COMBATE + dp.KILLS_COMANDANTE + dp.KILLS_NAO_COMBATE + dp.KILLS_OUTROS) as TOTAL_KILLS
FROM Fact_Desempenho_Partida dp
JOIN Fact_Partidas p ON dp.ID_PARTIDA = p.ID_PARTIDA
JOIN Dim_Jogadores dj ON dp.ID_JOGADOR = dj.ID_JOGADOR
LEFT JOIN Dim_Jogadores j_vencedor ON p.ID_JOGADOR_VENCEDOR = j_vencedor.ID_JOGADOR
LEFT JOIN Dim_Comandantes dc ON dp.ID_COMANDANTE = dc.ID_COMANDANTE;
"""

def run():
    print("Criando/Atualizando a view analítica 'vw_desempenho_completo'...")
    try:
        con = duckdb.connect(database=DB_FILE)
        con.execute(CREATE_VIEW_SQL)
        con.close()
        print("View criada com sucesso!")
    except Exception as e:
        print(f"Ocorreu um erro: {e}")

if __name__ == "__main__":
    run()