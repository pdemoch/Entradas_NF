import os
import requests
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from supabase import create_client
from dotenv import load_dotenv

# ==========================================
# 1. CONFIGURAÇÕES GERAIS E CONEXÕES
# ==========================================
load_dotenv()
st.set_page_config(page_title="Projeto Nexus", layout="wide")

@st.cache_resource
def get_supabase_client():
    """Inicializa a conexão buscando credenciais do Render ou do secrets.toml local"""
    url = os.environ.get("SUPABASE_URL") or st.secrets.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY") or st.secrets.get("SUPABASE_KEY")
    
    if not url or not key:
        st.error("⚠️ Credenciais do Supabase não encontradas.")
        st.stop()
        
    return create_client(url, key)

# ==========================================
# 2. CAMADA DE DADOS (EXTRAÇÃO E TRATAMENTO)
# ==========================================
@st.cache_data(ttl=600, show_spinner="Extraindo dados do Supabase...")
def fetch_data_from_supabase() -> pd.DataFrame:
    """Realiza a paginação e busca todos os dados brutos do banco"""
    supabase = get_supabase_client()
    data, offset, limit = [], 0, 1000
    
    while True:
        res = supabase.table("nfs_sop").select("*").range(offset, offset + limit - 1).execute()
        if not res.data: 
            break
        data.extend(res.data)
        if len(res.data) < limit: 
            break
        offset += limit
        
    return pd.DataFrame(data)

def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Centraliza todas as regras de negócio e limpeza do DataFrame (ETL)"""
    if df.empty:
        return df

    # Tratamento de Datas
    df['Data de receção'] = pd.to_datetime(df['d1_dtdigit'], format='%Y%m%d', errors='coerce').dt.normalize()
    df['Data de emissão'] = pd.to_datetime(df['d1_emissao'], format='%Y%m%d', errors='coerce').dt.normalize()
    
    # Conversão de Numéricos e Limpeza de Texto
    for col in ['d1_quant', 'd1_vunit', 'd1_total']:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')

    df['d1_tp'] = df['d1_tp'].astype(str).str.strip()
    df['d1_xdescri'] = df['d1_xdescri'].astype(str).str.strip()

    # Filtros e Ordenação base
    df = df.dropna(subset=['Data de receção', 'd1_total'])
    df = df.sort_values('Data de receção', ascending=True)

    # Lógica de Quadrantes otimizada usando pd.cut
    df['Dia'] = df['Data de receção'].dt.day
    df['Quadrante'] = pd.cut(
        df['Dia'], 
        bins=[0, 10, 20, 31], 
        labels=['Q1 (01 a 10)', 'Q2 (11 a 20)', 'Q3 (21 a 31)']
    ).astype(str)
    
    # Labels Mensais
    df['Mês_Ref'] = df['Data de receção'].dt.to_period('M').dt.to_timestamp()
    df['Mês/Ano Label'] = df['Data de receção'].dt.strftime('%m/%Y')

    return df

def formata_br(valor, moeda=False) -> str:
    """Utilitário para formatar números no padrão brasileiro"""
    if pd.isna(valor): return "0"
    texto = f"{float(valor):,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {texto}" if moeda else texto

# ==========================================
# 3. CAMADA DE SERVIÇOS (APIs EXTERNAS)
# ==========================================
def trigger_github_action() -> tuple[bool, str]:
    """Dispara o workflow do GitHub e retorna o status de sucesso"""
    gh_token = os.environ.get("GITHUB_TOKEN") or st.secrets.get("GITHUB_TOKEN")
    gh_owner = os.environ.get("GITHUB_OWNER") or st.secrets.get("GITHUB_OWNER")
    gh_repo = os.environ.get("GITHUB_REPO") or st.secrets.get("GITHUB_REPO")
    
    url = f"https://api.github.com/repos/{gh_owner}/{gh_repo}/actions/workflows/update_data.yml/dispatches"
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"Bearer {gh_token}"
    }
    
    try:
        response = requests.post(url, headers=headers, json={"ref": "master"})
        return response.status_code == 204, response.text
    except Exception as e:
        return False, str(e)

# ==========================================
# 4. COMPONENTES DE INTERFACE (UI)
# ==========================================
def render_sidebar(df: pd.DataFrame) -> pd.DataFrame:
    """Renderiza a barra lateral com filtros em cascata e painel de admin"""
    st.sidebar.header("🔍 Filtros Dinâmicos")
    
    df_filtrado = df.copy()
    
    if not df.empty:
        # Filtro de Tipo
        tipos = sorted(df_filtrado['d1_tp'].unique())
        tipos_selecionados = st.sidebar.multiselect("1. Selecione o Tipo (d1_tp):", options=tipos)
        
        if tipos_selecionados:
            df_filtrado = df_filtrado[df_filtrado['d1_tp'].isin(tipos_selecionados)]
            
        # Filtro Cascata de Descrição
        descricoes = sorted(df_filtrado['d1_xdescri'].unique())
        desc_selecionadas = st.sidebar.multiselect("2. Selecione a Descrição:", options=descricoes)
        
        if desc_selecionadas:
            df_filtrado = df_filtrado[df_filtrado['d1_xdescri'].isin(desc_selecionadas)]

    # Painel de Administração
    st.sidebar.markdown("---")
    st.sidebar.subheader("⚙️ Administração")
    
    if st.sidebar.button("🔄 Forçar Atualização de Dados", use_container_width=True):
        with st.spinner("Acordando o robô no GitHub..."):
            sucesso, erro_msg = trigger_github_action()
            if sucesso:
                st.sidebar.success("✅ Comando enviado! O banco será atualizado em ~2 minutos.")
                st.cache_data.clear()
            else:
                st.sidebar.error(f"Erro ao acionar o GitHub: {erro_msg}")
                
    return df_filtrado

def render_kpis(df: pd.DataFrame):
    """Renderiza a linha superior de métricas"""
    st.markdown("---")
    k1, k2, k3 = st.columns(3)
    
    total_fin = df['d1_total'].sum()
    vendas_q = df.groupby('Quadrante')['d1_total'].sum()
    
    def calc_rep(q_name):
        return f"{(vendas_q.get(q_name, 0)/total_fin)*100:.1f}%" if total_fin > 0 else "0%"

    k1.metric("Representatividade Q1", calc_rep('Q1 (01 a 10)'))
    k2.metric("Representatividade Q2", calc_rep('Q2 (11 a 20)'))
    k3.metric("Representatividade Q3", calc_rep('Q3 (21 a 31)'))
    st.markdown("---")

def render_heatmap(df: pd.DataFrame):
    """Renderiza o gráfico de calor e a tabela resumo"""
    df_pivot = df.groupby(['Mês_Ref', 'Quadrante'])['d1_total'].sum().unstack(fill_value=0)
    cols_q = ['Q1 (01 a 10)', 'Q2 (11 a 20)', 'Q3 (21 a 31)']
    df_pivot = df_pivot.reindex(columns=cols_q, fill_value=0).sort_index()

    df_pct_mes = df_pivot.div(df_pivot.sum(axis=1), axis=0).fillna(0)
    labels_y = df_pivot.index.strftime('%m/%Y')

    st.subheader("🟦 Evolução de Concentração por Quadrante")
    c_mapa, c_tab = st.columns([6, 4])

    with c_mapa:
        text_matrix = df_pivot.map(lambda x: formata_br(x, moeda=True))
        fig = go.Figure(data=go.Heatmap(
            z=df_pct_mes.values, x=df_pivot.columns, y=labels_y,
            text=text_matrix.values, texttemplate="%{text}", colorscale="Blues",
            hovertemplate="Mês: %{y}<br>Quadrante: %{x}<br>Volume: %{text}<extra></extra>"
        ))
        fig.update_layout(yaxis=dict(autorange="reversed"), height=600)
        st.plotly_chart(fig, use_container_width=True)

    with c_tab:
        st.write("### Somatória Mensal (R$)")
        df_display = df_pivot.copy()
        for col in df_display.columns:
            df_display[col] = df_display[col].map(lambda x: formata_br(x, True))
        df_display['Total'] = df_pivot.sum(axis=1).map(lambda x: formata_br(x, True))
        df_display.index = labels_y
        st.dataframe(df_display.reset_index().rename(columns={'index': 'Mês'}), hide_index=True, use_container_width=True)

def render_datatable(df: pd.DataFrame):
    """Renderiza a tabela analítica de fundo"""
    st.markdown("---")
    st.subheader(f"📋 Detalhamento das Entradas ({len(df)} registros)")
    
    df_final = df.rename(columns={
        'd1_filial': 'Filial', 'd1_cod': 'Produto', 'd1_xdescri': 'Descrição',
        'd1_quant': 'Quantidade', 'd1_vunit': 'Vlr Unitário', 'd1_total': 'Vlr Total',
        'd1_doc': 'NF', 'd1_tp': 'Tipo', 'd1_pedido': 'Pedido'
    })

    df_final['Emissão'] = df['Data de emissão'].dt.strftime('%d/%m/%Y')
    df_final['Digitação'] = df['Data de receção'].dt.strftime('%d/%m/%Y')
    df_final['Vlr Total'] = df_final['Vlr Total'].map(lambda x: formata_br(x, True))
    
    cols_view = ['Filial', 'Produto', 'Descrição', 'Quadrante', 'Pedido', 'NF', 'Vlr Total', 'Digitação', 'Tipo']
    st.dataframe(df_final[cols_view], hide_index=True, use_container_width=True)

# ==========================================
# 5. ORQUESTRADOR PRINCIPAL
# ==========================================
def main():
    st.title("📊 Painel de Concentração Financeira")
    st.caption("Monitoramento Integral (ME, MP, EM, SI)")

    # 1. Puxa e Trata Dados
    raw_df = fetch_data_from_supabase()
    processed_df = process_dataframe(raw_df)

    # 2. Renderiza Menu Lateral e Aplica Filtros
    df_filtrado = render_sidebar(processed_df)

    # 3. Renderiza Corpo do Painel
    if df_filtrado.empty:
        st.warning("⚠️ Nenhum dado encontrado (ou banco vazio/filtros muito restritos).")
        return

    render_kpis(df_filtrado)
    render_heatmap(df_filtrado)
    render_datatable(df_filtrado)

if __name__ == "__main__":
    main()