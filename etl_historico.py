import pandas as pd
import duckdb
from thefuzz import process
import regex as re
import hashlib
from datetime import datetime

# --- Constantes de Configuração ---
DB_FILE = "banco_mtg.db"
DATA_PATH = "dados_historicos/"
EXCEL_FILE = "MTG-ST-2025.xlsx"
EXCEL_PATH = f"{DATA_PATH}{EXCEL_FILE}"

# --- Funções Auxiliares ---

def get_commander_map(con):
    """Busca o mapa de Nomes -> IDs de Comandantes do banco."""
    df = con.execute("SELECT NOME_COMANDANTE_EN, ID_COMANDANTE FROM Dim_Comandantes").fetchdf()
    return pd.Series(df['ID_COMANDANTE'].values, index=df['NOME_COMANDANTE_EN']).to_dict()

def get_metodos_map(con):
    """Busca o mapa de Código -> ID de Métodos de Eliminação."""
    df = con.execute("SELECT CODIGO_METODO, ID_METODO FROM Dim_Metodos_Eliminacao").fetchdf()
    return pd.Series(df['ID_METODO'].values, index=df['CODIGO_METODO']).to_dict()

def match_commander(name, choices, threshold=85):
    """Faz o fuzzy match de um nome de comandante."""
    if not name or pd.isna(name): return None
    clean_name = re.sub(r'\s*\(.*\)\s*$', '', name).strip()
    match, score = process.extractOne(clean_name, choices.keys())
    if score >= threshold:
        return choices[match]
    return None

def parse_player_commander(text):
    """Extrai (Comandante, Jogador) de uma string 'Comandante (Jogador)'."""
    if not isinstance(text, str):
        return None, None
    match = re.search(r'^(.*?)\s*\((.*?)\)$', text)
    if match:
        return match.group(1).strip(), match.group(2).strip()
    return text.strip(), None

def parse_historico(row, jogadores_map, comandantes_map, metodos_map):
    """Função para parsear a linha de dados de uma partida da aba REG."""
    data_partida = row.get('Data')
    if pd.isna(data_partida): return None

    # Gera ID da Partida
    vencedor_str = row.get('DECK1')
    if pd.isna(vencedor_str): return None
    partida_hash = hashlib.md5(f"{data_partida.isoformat()}{vencedor_str}".encode()).hexdigest()
    id_partida = int(partida_hash, 16) % (10**12)

    # Extrai vencedor
    _, nome_vencedor = parse_player_commander(vencedor_str)
    id_vencedor = jogadores_map.get(nome_vencedor)

    partida_info = {
        'id_partida': id_partida,
        'data': data_partida.date(),
        'num_jogadores': row.get('Nº J'),
        'id_vencedor': id_vencedor
    }
    
    desempenhos = []
    # Processa cada jogador na partida (até 5)
    for i in range(1, 6):
        posicao = i
        col_deck = f'DECK{i}'
        
        if col_deck in row and pd.notna(row[col_deck]):
            nome_comandante, nome_jogador = parse_player_commander(row[col_deck])
            
            # Lógica para extrair a soma dos métodos de eliminação para este jogador
            kills_c = row.get(f'C{i}', 0)
            kills_d = row.get(f'D{i}', 0)
            kills_n = row.get(f'N{i}', 0)
            kills_o = row.get(f'O{i}', 0)
            
            desempenhos.append({
                'id_partida': id_partida,
                'id_jogador': jogadores_map.get(nome_jogador),
                'id_comandante': match_commander(nome_comandante, comandantes_map),
                'posicao': posicao,
                'pontuacao': row.get(f'Pt{i}'),
                'kills_combate': kills_c,
                'kills_comandante': kills_d,
                'kills_nao_combate': kills_n,
                'kills_outros': kills_o
            })
            
    return partida_info, desempenhos

# --- Função Principal do ETL ---

def etl_historico():
    print("Iniciando ETL de dados históricos...")
    
    try:
        con = duckdb.connect(database=DB_FILE)
        
        # --- 1. JOGADORES ---
        print("Lendo a aba 'players'...")
        df_jogadores = pd.read_excel(EXCEL_PATH, sheet_name='players')
        df_jogadores.rename(columns={'NOME': 'NOME_JOGADOR'}, inplace=True)
        df_jogadores.dropna(subset=['NOME_JOGADOR'], inplace=True)
        df_jogadores.insert(0, 'ID_JOGADOR', range(1, 1 + len(df_jogadores)))
        con.executemany("INSERT OR IGNORE INTO Dim_Jogadores (ID_JOGADOR, NOME_JOGADOR) VALUES (?, ?)", df_jogadores.values.tolist())
        print(f"{len(df_jogadores)} jogadores carregados para Dim_Jogadores.")
        jogadores_map = pd.Series(df_jogadores['ID_JOGADOR'].values, index=df_jogadores['NOME_JOGADOR']).to_dict()

        # --- 2. COMANDANTES ---
        comandantes_map = get_commander_map(con)
        print(f"{len(comandantes_map)} comandantes carregados do banco para matching.")

        # --- 3. PROCESSAMENTO DA ABA 'REG' ---
        df_reg = pd.read_excel(EXCEL_PATH, sheet_name='REG')
        print(f"Lendo {len(df_reg)} partidas da aba 'REG'.")
        
        all_partidas_info = []
        all_desempenho_info = []

        for index, row in df_reg.iterrows():
            # Usando os nomes de coluna corretos revelados pelo log de depuração
            data_partida = row.get('DATA')
            if pd.isna(data_partida): continue

            # Gera ID da Partida
            vencedor_str = row.get('DECK1') # <-- CORREÇÃO
            if pd.isna(vencedor_str): continue
            partida_hash = hashlib.md5(f"{data_partida.isoformat()}{vencedor_str}".encode()).hexdigest()
            id_partida = int(partida_hash, 16) % (10**12)

            # Extrai vencedor
            _, nome_vencedor = parse_player_commander(vencedor_str)
            id_vencedor = jogadores_map.get(nome_vencedor)

            # Adiciona à lista de partidas
            all_partidas_info.append({
                'id_partida': id_partida,
                'data': data_partida.date(),
                'num_jogadores': row.get('Nº J'), # <-- CORREÇÃO
                'id_vencedor': id_vencedor
            })

            # Processa cada jogador na partida (até 5)
            for i in range(1, 6):
                posicao = i
                col_deck = f'DECK{i}' # <-- CORREÇÃO (ex: DECK1, DECK2, etc.)
                
                if col_deck in row and pd.notna(row[col_deck]):
                    nome_comandante, nome_jogador = parse_player_commander(row[col_deck])
                    
                    all_desempenho_info.append({
                        'id_partida': id_partida,
                        'id_jogador': jogadores_map.get(nome_jogador),
                        'id_comandante': match_commander(nome_comandante, comandantes_map),
                        'posicao': posicao,
                        'pontuacao': row.get(f'Pt{i}'), # <-- CORREÇÃO
                        'kills_combate': row.get(f'C{i}', 0),
                        'kills_comandante': row.get(f'D{i}', 0),
                        'kills_nao_combate': row.get(f'N{i}', 0),
                        'kills_outros': row.get(f'O{i}', 0)
                    })
        
        print("Parse do histórico finalizado.")

        # --- 4. CARGA NO BANCO DE DADOS ---
        if all_partidas_info:
            df_partidas = pd.DataFrame(all_partidas_info).drop_duplicates(subset=['id_partida'])
            con.executemany("INSERT OR IGNORE INTO Fact_Partidas (ID_PARTIDA, DATA_PARTIDA, NUM_JOGADORES, ID_JOGADOR_VENCEDOR) VALUES (?, ?, ?, ?)", df_partidas.values.tolist())
            print(f"{len(df_partidas)} partidas inseridas em Fact_Partidas.")

        if all_desempenho_info:
            df_desempenho = pd.DataFrame(all_desempenho_info)
            df_desempenho.insert(0, 'ID_DESEMPENHO', range(1, 1 + len(df_desempenho)))
            
            df_desempenho.dropna(subset=['id_comandante'], inplace=True)
            print(f"Filtrando para {len(df_desempenho)} registros de desempenho com comandante correspondido.")

            colunas_numericas = ['pontuacao', 'kills_combate', 'kills_comandante', 'kills_nao_combate', 'kills_outros']
            for col in colunas_numericas:
                if col in df_desempenho.columns:
                    df_desempenho[col] = df_desempenho[col].fillna(0).astype(int)

            con.executemany("INSERT OR IGNORE INTO Fact_Desempenho_Partida VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", df_desempenho.values.tolist())
            print(f"{len(df_desempenho)} registros de desempenho inseridos em Fact_Desempenho_Partida.")

    except Exception as e:
        print(f"ERRO durante o processo de ETL: {e}")
    finally:
        if 'con' in locals() and con:
            con.close()
            print("Conexão com o banco de dados fechada.")

if __name__ == "__main__":
    etl_historico()