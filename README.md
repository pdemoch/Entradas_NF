# 🌐 Projeto Nexus | Supply Chain & Cash Flow Synchronizer

![Status](https://img.shields.io/badge/Status-Operacional-brightgreen?style=for-the-badge)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=Streamlit&logoColor=white)
![Supabase](https://img.shields.io/badge/Supabase-3ECF8E?style=for-the-badge&logo=supabase&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=for-the-badge&logo=github-actions&logoColor=white)

O **Nexus** é uma plataforma de inteligência de dados desenvolvida para a **Linea Alimentos**, focada na gestão estratégica de recebimentos e otimização do fluxo de caixa. O sistema monitora a entrada de insumos no estoque monetário, permitindo uma análise granular da linearidade dos fornecedores.

---

## 🎯 Visão Estratégica: Gestão de Capital de Giro
O objetivo central do Nexus é garantir a **linearidade dos recebimentos**. Em uma operação de S&OP de alto nível, evitar picos de entrada de NF é crítico para manter um fluxo de caixa saudável e uma operação logística equilibrada.

### Pilares do Processo:
* **Data de Digitação:** O ponto da verdade. Identifica o momento exato em que o insumo (MP, ME ou SI) entra no estoque físico e contábil.
* **Estoque Monetário:** Visualização do impacto financeiro imediato das notas fiscais por quadrante do mês (Q1, Q2 e Q3).
* **Categorização de Insumos:** Análise segregada para **MP** (Matéria-Prima), **ME** (Material de Embalagem) e **SI** (Serviço de Industrialização).

---

## 🏗️ Arquitetura de Dados (Modern Data Stack)
A solução foi desenhada para ser totalmente *serverless*, com custo zero de processamento e alta escalabilidade:

1.  **Extração (ETL):** Script Python assíncrono que consome a API Gobi, capturando um histórico rolling de 14 meses.
2.  **Orquestração:** **GitHub Actions** atuando como maestro, executando o pipeline 3x ao dia (08h00, 14h00, 16h30 BRT).
3.  **Data Warehouse:** **Supabase (PostgreSQL)** para persistência de dados e consultas rápidas.
4.  **Business Intelligence:** Dashboard em **Streamlit** com processamento paginado (offset) para visualização de grandes volumes de dados.

---

## ✨ Funcionalidades Chave

| Funcionalidade | Descrição de Valor |
| :--- | :--- |
| **Mapa de Calor de Recebimentos** | Identifica visualmente onde o volume financeiro está concentrado no mês, alertando para desvios de fluxo de caixa. |
| **Filtros Dinâmicos em Cascata** | Seleção por Tipo (`d1_tp`) que filtra automaticamente as descrições de produtos, permitindo drill-down rápido. |
| **Trigger Remoto (Botão Mágico)** | Interface direta com o GitHub Actions para forçar a atualização dos dados via API sem sair do dashboard. |
| **Carga Paginada** | Motor de dados robusto que supera limitações de API para processamento integral da base (4.7k+ registros). |

---

## 🚀 Como Executar Localmente

1.  **Clone o repositório:**
    ```bash
    git clone [https://github.com/pdemoch/Entradas_NF.git](https://github.com/pdemoch/Entradas_NF.git)
    ```
2.  **Instale as dependências:**
    ```bash
    pip install -r requirements.txt
    ```
3.  **Configuração de Secrets:**
    Certifique-se de configurar as variáveis no seu `.streamlit/secrets.toml` ou ambiente local:
    * `SUPABASE_URL` / `SUPABASE_KEY`
    * `GOBI_TOKEN`
    * `GITHUB_TOKEN`

4.  **Execute o Dashboard:**
    ```bash
    streamlit run app.py
    ```

---

## 👨‍💻 Autor
**Phillipe Silva** - Analista de S&OP na Linea Alimentos.  
Desenvolvendo soluções que unem Planejamento de Demanda, Finanças e Engenharia de Dados para otimização de EBITDA.

---

> **Nota de Segurança:** As chaves de API e Tokens de acesso são gerenciados via Repository Secrets e não estão expostos no código fonte.
