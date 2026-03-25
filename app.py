import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from supabase import create_client
import os
import requests  # Necessário para o botão de atualizar o GitHub

# 1. CONFIGURAÇÃO BASE
st.set_page_config(page_title="Projeto Nexus", layout="wide")

@st.cache_resource
def init_connection():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase = init_connection()

# 2. MOTOR DE DADOS COM PAGINAÇÃO
@st.cache_data(ttl=600, show_spinner="Carregando Base Completa do Supabase...")
def carregar_dados_supabase():
    data, offset = [], 0
    
    while True:
        res = supabase.table("nfs_sop").select("*").range(offset, offset + 999).execute()
        if not res.data: 
            break
        data.extend(res.data)
        if len(res.data) < 1000: 
            break
        offset += 1000
    
    df = pd.DataFrame(data)
    
    if df.empty:
        return pd.DataFrame()

    # TRATAMENTO DE DATAS E TEXTOS
    df['Data de receção'] = pd.to_datetime(df['d1_dtdigit'], format='%Y%m%d', errors='coerce').dt.normalize()
    df['Data de emissão'] = pd.to_datetime(df['d1_emissao'], format='%Y%m%d', errors='coerce').dt.normalize()
    
    for col in ['d1_quant', 'd1_vunit', 'd1_total']:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')

    # Limpeza de espaços invisíveis para os filtros funcionarem perfeitamente
    df['d1_tp'] = df['d1_tp'].astype(str).str.strip()
    df['d1_xdescri'] = df['d1_xdescri'].astype(str).str.strip()

    df = df.dropna(subset=['Data de receção', 'd1_total'])
    df = df.sort_values('Data de receção', ascending=True)

    df['Dia'] = df['Data de receção'].dt.day
    df['Quadrante'] = df['Dia'].apply(lambda d: 'Q1 (01 a 10)' if d <= 10 else ('Q2 (11 a 20)' if d <= 20 else 'Q3 (21 a 31)'))
    
    df['Mês_Ref'] = df['Data de receção'].dt.to_period('M').dt.to_timestamp()
    df['Mês/Ano Label'] = df['Data de receção'].dt.strftime('%m/%Y')

    return df

def formata_br(valor, moeda=False):
    if pd.isna(valor): return "0"
    texto = f"{float(valor):,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {texto}" if moeda else texto

# ==========================================
# APLICAÇÃO E INTERFACE
# ==========================================
df = carregar_dados_supabase()

# --- BARRA LATERAL: FILTROS E ADMINISTRAÇÃO ---
st.sidebar.header("🔍 Filtros Dinâmicos")

if not df.empty:
    # 1. Filtro de Tipo
    tipos_disponiveis = sorted(df['d1_tp'].unique())
    tipos_selecionados = st.sidebar.multiselect("1. Selecione o Tipo (d1_tp):", options=tipos_disponiveis, default=[])
    
    df_filtrado = df.copy()
    if tipos_selecionados:
        df_filtrado = df_filtrado[df_filtrado['d1_tp'].isin(tipos_selecionados)]
        
    # 2. Filtro de Descrição (Cascata: mostra só o que pertence ao tipo escolhido)
    descricoes_disponiveis = sorted(df_filtrado['d1_xdescri'].unique())
    descricoes_selecionadas = st.sidebar.multiselect("2. Selecione a Descrição:", options=descricoes_disponiveis, default=[])
    
    if descricoes_selecionadas:
        df_filtrado = df_filtrado[df_filtrado['d1_xdescri'].isin(descricoes_selecionadas)]
else:
    df_filtrado = pd.DataFrame()

# --- BOTÃO DE ATUALIZAÇÃO MANUAL (GITHUB ACTIONS) ---
st.sidebar.markdown("---")
st.sidebar.subheader("⚙️ Administração")

if st.sidebar.button("🔄 Forçar Atualização de Dados", use_container_width=True):
    with st.spinner("Acordando o robô no GitHub..."):
        try:
            gh_token = st.secrets["GITHUB_TOKEN"]
            gh_owner = st.secrets["GITHUB_OWNER"]
            gh_repo = st.secrets["GITHUB_REPO"]
            
            # ATENÇÃO: Verifique se o nome do seu arquivo lá no GitHub é .yml ou .yaml
            # Se for .yml, mude o final do link abaixo para update_data.yml
            url = f"https://api.github.com/repos/{gh_owner}/{gh_repo}/actions/workflows/update_data.yml/dispatches"
            
            headers = {
                "Accept": "application/vnd.github.v3+json",
                "Authorization": f"Bearer {gh_token}"
            }
            # Aqui disparamos na branch principal 'master'
            data = {"ref": "master"}
            
            response = requests.post(url, headers=headers, json=data)
            
            if response.status_code == 204:
                st.sidebar.success("✅ Comando enviado! O banco será atualizado em ~2 minutos. Dê F5 na página depois.")
                # Limpa o cache do Streamlit para forçar a leitura dos dados novos no próximo F5
                st.cache_data.clear()
            else:
                st.sidebar.error(f"Erro ao acionar o GitHub: {response.text}")
                
        except Exception as e:
            st.sidebar.error(f"Erro de configuração: {e}")

# --- CORPO DO DASHBOARD ---
st.title("📊 Painel de Concentração Financeira")
st.caption("Monitoramento Integral (ME, MP, EM, SI)")

if df_filtrado.empty:
    st.warning("⚠️ Nenhum dado encontrado (ou banco vazio/filtros muito restritos).")
else:
    # --- KPIs ---
    st.markdown("---")
    k1, k2, k3 = st.columns(3)
    
    total_fin = df_filtrado['d1_total'].sum()
    vendas_q = df_filtrado.groupby('Quadrante')['d1_total'].sum()
    
    k1.metric("Representatividade Q1", f"{(vendas_q.get('Q1 (01 a 10)', 0)/total_fin)*100:.1f}%" if total_fin > 0 else "0%")
    k2.metric("Representatividade Q2", f"{(vendas_q.get('Q2 (11 a 20)', 0)/total_fin)*100:.1f}%" if total_fin > 0 else "0%")
    k3.metric("Representatividade Q3", f"{(vendas_q.get('Q3 (21 a 31)', 0)/total_fin)*100:.1f}%" if total_fin > 0 else "0%")
    st.markdown("---")

    # --- MAPA DE CALOR ---
    df_pivot = df_filtrado.groupby(['Mês_Ref', 'Quadrante'])['d1_total'].sum().unstack(fill_value=0)
    cols_q = ['Q1 (01 a 10)', 'Q2 (11 a 20)', 'Q3 (21 a 31)']
    df_pivot = df_pivot.reindex(columns=cols_q, fill_value=0).sort_index()

    df_pct_mes = df_pivot.div(df_pivot.sum(axis=1), axis=0).fillna(0)
    labels_y = df_pivot.index.strftime('%m/%Y')

    st.subheader("🟦 Evolução de Concentração por Quadrante")
    c_mapa, c_tab = st.columns([6, 4])

    with c_mapa:
        text_matrix = df_pivot.applymap(lambda x: formata_br(x, moeda=True))
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
            df_display[col] = df_display[col].apply(lambda x: formata_br(x, True))
        df_display['Total'] = df_pivot.sum(axis=1).apply(lambda x: formata_br(x, True))
        df_display.index = labels_y
        st.dataframe(df_display.reset_index().rename(columns={'index': 'Mês'}), hide_index=True, use_container_width=True)

    # --- TABELA DE DETALHAMENTO ---
    st.markdown("---")
    st.subheader(f"📋 Detalhamento das Entradas ({len(df_filtrado)} registros)")
    
    df_final = df_filtrado.rename(columns={
        'd1_filial': 'Filial', 'd1_cod': 'Produto', 'd1_xdescri': 'Descrição',
        'd1_quant': 'Quantidade', 'd1_vunit': 'Vlr Unitário', 'd1_total': 'Vlr Total',
        'd1_doc': 'NF', 'd1_tp': 'Tipo', 'd1_pedido': 'Pedido'
    })

    df_final['Emissão'] = df_filtrado['Data de emissão'].dt.strftime('%d/%m/%Y')
    df_final['Digitação'] = df_filtrado['Data de receção'].dt.strftime('%d/%m/%Y')
    df_final['Vlr Total'] = df_final['Vlr Total'].apply(lambda x: formata_br(x, True))
    
    cols_view = ['Filial', 'Produto', 'Descrição', 'Quadrante', 'Pedido', 'NF', 'Vlr Total', 'Digitação', 'Tipo']
    st.dataframe(df_final[cols_view], hide_index=True, use_container_width=True)