import asyncio
import aiohttp
import pandas as pd
import os
from datetime import date
from supabase import create_client
from dotenv import load_dotenv

# Carrega as variáveis do .env
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GOBI_TOKEN = os.getenv("GOBI_TOKEN")
GOBI_URL = "https://gobi-api.lineaalimentos.com.br/v1/reports/565/data"

# Configuração do limitador (5 requisições simultâneas)
MAX_CONCURRENCY = 5
semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

# Inicializa Supabase
if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERRO: Credenciais do Supabase não encontradas no .env")
    exit()

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

async def fetch_with_semaphore(session, d):
    async with semaphore:
        params = {"start_date": d, "end_date": d, "streaming": "true", "format": "json"}
        try:
            async with session.get(GOBI_URL, params=params, timeout=60) as response:
                if response.status == 200:
                    try:
                        return await response.json()
                    except Exception:
                        # Caso a API retorne Status 200 mas com corpo inválido/vazio
                        return []
                elif response.status == 401:
                    print(f"Erro no dia {d}: Não autorizado (401). Verifique o Token.")
                    return []
                else:
                    print(f"Erro no dia {d}: Status {response.status}")
                    return []
        except Exception as e:
            print(f"Falha na conexão no dia {d}: {e}")
            return []

async def fetch_data():
    hoje = pd.to_datetime(date.today())
    # Garante os últimos 14 meses começando no dia 1º
    inicio = (hoje - pd.DateOffset(months=14)).replace(day=1)
    datas = pd.date_range(start=inicio, end=hoje).strftime("%Y-%m-%d").tolist()
    
    print(f"Iniciando extração de {len(datas)} dias da API Gobi...")
    
    headers = {"Authorization": GOBI_TOKEN}
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENCY)
    
    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
        tasks = [fetch_with_semaphore(session, d) for d in datas]
        resultados = await asyncio.gather(*tasks)
        
        all_data = []
        for res in resultados:
            if res and isinstance(res, list):
                all_data.extend(res)
        return all_data

def transform_and_upload(raw_data):
    df = pd.DataFrame(raw_data)
    
    if df.empty:
        print("Nenhum dado válido extraído para transformar.")
        return

    # 1. Filtro de Pedido (Removendo lixo e vazios)
    df['d1_pedido'] = df['d1_pedido'].astype(str).str.strip()
    df = df[~df['d1_pedido'].str.lower().isin(['nan', 'none', ''])]
    
    # 2. Definição estrita das colunas (Nexus SOP v4.0)
    # Essas colunas DEVEM existir no Supabase para não dar erro PGRST204
    cols_necessarias = [
        'd1_filial', 'd1_cod', 'd1_xdescri', 'd1_um', 'd1_quant', 'd1_vunit', 
        'd1_total', 'd1_pedido', 'd1_doc', 'd1_emissao', 'd1_dtdigit', 'd1_tp',
        'd1_cf', 'd1_serie', 'd1_numseq', 'd1_local', 'd1_tes', 'd1_lotectl',
        'd1_dtvalid', 'd1_fornece', 'd1_dfabric'
    ]
    
    # Preenche colunas que possam faltar no retorno da API com valor vazio
    for col in cols_necessarias:
        if col not in df.columns:
            df[col] = ""

    # Mantém apenas as colunas que vamos subir
    df_final = df[cols_necessarias].copy()
    
    # 3. Limpeza do Banco e Upload
    print(f"Limpando banco e preparando upload de {len(df_final)} registros...")
    try:
        # Deleta registros antigos (rolling 14 meses)
        supabase.table("nfs_sop").delete().neq("d1_doc", "TEMP_CLEAN").execute()
    except Exception as e:
        print(f"Aviso na limpeza do banco: {e}")

    # Transforma para lista de dicionários
    records = df_final.to_dict('records')
    batch_size = 500
    
    print(f"Iniciando upload em lotes de {batch_size}...")
    for i in range(0, len(records), batch_size):
        lote = records[i:i + batch_size]
        try:
            supabase.table("nfs_sop").insert(lote).execute()
            print(f"Progresso: {min(i + batch_size, len(records))}/{len(records)} enviados.")
        except Exception as e:
            print(f"Erro fatal ao inserir lote no Supabase: {e}")
            print("Verifique se as colunas no banco de dados coincidem com o script.")
            break

if __name__ == "__main__":
    if not GOBI_TOKEN:
        print("ERRO: GOBI_TOKEN não encontrado no arquivo .env")
    else:
        dados = asyncio.run(fetch_data())
        if dados:
            transform_and_upload(dados)
            print("=== Carga finalizada com sucesso! ===")
        else:
            print("A API não retornou dados. Verifique se o seu Token é válido ou se há conexão.")