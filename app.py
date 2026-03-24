import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from supabase import create_client
import os

# 1. CONFIGURAÇÃO BASE
st.set_page_config(page_title="Projeto Nexus", layout="wide")

@st.cache_resource
def init_connection():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase = init_connection()

# 2. MOTOR DE DADOS COM PAGINAÇÃO (CONFORME SEU EXEMPLO)
@st.cache_data(ttl=600, show_spinner="Carregando Base Completa do Supabase...")
def carregar_dados_supabase():
    data, offset = [], 0
    
    # Laço para buscar todas as linhas (sem limite de 1000)
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

    # TRATAMENTO DE DATAS (Normalização e Formatação)
    df['Data de receção'] = pd.to_datetime(df['d1_dtdigit'], format='%Y%m%d', errors='coerce').dt.normalize()
    df['Data de emissão'] = pd.to_datetime(df['d1_emissao'], format='%Y%m%d', errors='coerce').dt.normalize()
    
    # Conversão de Valores
    for col in ['d1_quant', 'd1_vunit', 'd1_total']:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')

    df = df.dropna(subset=['Data de receção', 'd1_total'])
    df = df.sort_values('Data de receção', ascending=True)

    # LÓGICA DE QUADRANTES (Decêndios)
    df['Dia'] = df['Data de receção'].dt.day
    df['Quadrante'] = df['Dia'].apply(lambda d: 'Q1 (01 a 10)' if d <= 10 else ('Q2 (11 a 20)' if d <= 20 else 'Q3 (21 a 31)'))
    
    # Labels de agrupamento Mensal
    df['Mês_Ref'] = df['Data de receção'].dt.to_period('M').dt.to_timestamp()
    df['Mês/Ano Label'] = df['Data de receção'].dt.strftime('%m/%Y')

    return df

def formata_br(valor, moeda=False):
    if pd.isna(valor): return "0"
    texto = f"{float(valor):,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {texto}" if moeda else texto

# 3. INTERFACE E VISUAIS
st.title("📊 Painel de Concentração Financeira")
st.caption("Monitoramento Integral (ME, MP, EM, SI)")

df = carregar_dados_supabase()

if df.empty:
    st.warning("⚠️ Banco de dados vazio ou erro de conexão. Verifique o Supabase.")
else:
    # --- KPIs: REPRESENTATIVIDADE GLOBAL ---
    st.markdown("---")
    k1, k2, k3 = st.columns(3)
    
    total_fin = df['d1_total'].sum()
    vendas_q = df.groupby('Quadrante')['d1_total'].sum()
    
    k1.metric("Representatividade Q1", f"{(vendas_q.get('Q1 (01 a 10)', 0)/total_fin)*100:.1f}%")
    k2.metric("Representatividade Q2", f"{(vendas_q.get('Q2 (11 a 20)', 0)/total_fin)*100:.1f}%")
    k3.metric("Representatividade Q3", f"{(vendas_q.get('Q3 (21 a 31)', 0)/total_fin)*100:.1f}%")
    st.markdown("---")

    # --- MAPA DE CALOR E SOMATÓRIA ---
    df_pivot = df.groupby(['Mês_Ref', 'Quadrante'])['d1_total'].sum().unstack(fill_value=0)
    cols_q = ['Q1 (01 a 10)', 'Q2 (11 a 20)', 'Q3 (21 a 31)']
    df_pivot = df_pivot.reindex(columns=cols_q, fill_value=0).sort_index()

    # Intensidade da cor (gradiente azul por mês)
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

    # --- TABELA DE DETALHAMENTO COMPLETA ---
    st.markdown("---")
    st.subheader(f"📋 Detalhamento das Entradas ({len(df)} registros carregados)")
    
    df_final = df.rename(columns={
        'd1_filial': 'Filial', 'd1_cod': 'Produto', 'd1_xdescri': 'Descrição',
        'd1_quant': 'Quantidade', 'd1_vunit': 'Vlr Unitário', 'd1_total': 'Vlr Total',
        'd1_doc': 'NF', 'd1_tp': 'Tipo', 'd1_pedido': 'Pedido'
    })

    df_final['Emissão'] = df['Data de emissão'].dt.strftime('%d/%m/%Y')
    df_final['Digitação'] = df['Data de receção'].dt.strftime('%d/%m/%Y')
    df_final['Vlr Total'] = df_final['Vlr Total'].apply(lambda x: formata_br(x, True))
    
    cols_view = ['Filial', 'Produto', 'Descrição', 'Quadrante', 'Pedido', 'NF', 'Vlr Total', 'Digitação', 'Tipo']
    st.dataframe(df_final[cols_view], hide_index=True, use_container_width=True)