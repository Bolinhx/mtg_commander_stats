import requests
import duckdb
import time

DB_FILE = "banco_mtg.db"
SCRYFALL_API_URL = "https://api.scryfall.com/cards/search"
QUERY = '(type:legendary AND type:creature) OR (oracle:"can be your commander")'

def fetch_all_commanders():
    print("Iniciando busca de comandantes na API do Scryfall...")

    all_cards = []
    # Parâmetros da busca.
    params = {'q': QUERY}

    next_page_url = SCRYFALL_API_URL

    while next_page_url:
        try:
            response = requests.get(next_page_url, params=params)
            response.raise_for_status()
            data = response.json()

            cards_data = data.get('data', [])
            all_cards.extend(cards_data)

            print(f"  {len(cards_data)} cartas recebidas. Total acumulado: {len(all_cards)}")

            if data.get('has_more'):
                next_page_url = data['next_page']
                params = {} 
            else:
                next_page_url = None

            time.sleep(0.1) 

        except requests.exceptions.RequestException as e:
            print(f"ERRO: Falha ao conectar com a API do Scryfall: {e}")
            return None

    print(f"Busca finalizada! Total de {len(all_cards)} comandantes encontrados.")
    return all_cards

def save_commanders_to_db(commanders_data):
    if not commanders_data:
        print("Nenhum dado de comandante para salvar.")
        return

    print("Processando e salvando comandantes no banco de dados...")

    processed_commanders = []
    for card in commanders_data:
        # Pega o nome apenas da primeira face em caso de cartas duplas (ex: MDFC)
        commander_name = card['name'].split(' // ')[0]

        processed_commanders.append({
            'ID_COMANDANTE': card['oracle_id'],
            'NOME_COMANDANTE_EN': commander_name,
            'IDENTIDADE_DE_COR': "".join(card['color_identity']),
            'TIPO_CARTA': card['type_line'],
            'URL_IMAGEM': card.get('image_uris', {}).get('normal')
        })

    try:
        con = duckdb.connect(database=DB_FILE)
        
        con.executemany(
            "INSERT INTO Dim_Comandantes VALUES (?, ?, ?, ?, ?)", 
            [list(c.values()) for c in processed_commanders]
        )
        con.close()
        print(f"{len(processed_commanders)} comandantes salvos com sucesso na tabela Dim_Comandantes.")
    except Exception as e:
        print(f"ERRO ao salvar no banco de dados: {e}")

if __name__ == "__main__":
    all_commanders = fetch_all_commanders()
    if all_commanders:
        save_commanders_to_db(all_commanders)