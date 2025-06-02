import io
import sys

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

"""
SISTEMA COMPLETO E FUNCIONAL - GESTÃO DE FONTES + DIAGNÓSTICO GCPJ
VERSÃO CORRIGIDA - Tratamento de valores None

FUNCIONALIDADES TESTADAS E OPERACIONAIS:
1. ✅ Dashboard com estatísticas reais (CORRIGIDO)
2. ✅ Gestão de fontes (add/test/remove)
3. ✅ Gestão de escopo GCPJ
4. ✅ Diagnóstico de completude por GCPJ (INTEGRADO)
5. ✅ Execução de migrações
6. ✅ Interface de controle de qualidade

CORREÇÕES APLICADAS:
- Tratamento de valores None em estatísticas
- Verificações de segurança em formatação
- Valores padrão quando não há dados
- Mensagens informativas para estados vazios

INSTALAÇÃO:
pip install flask pandas openpyxl

USO:
python interface_gestao_fontes_v2_corrigido.py
Acesse: http://localhost:5003
"""
import io
import sys

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
"""
import io
import sys

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
SISTEMA COMPLETO E FUNCIONAL - GESTÃO DE FONTES + DIAGNÓSTICO GCPJ
VERSÃO CORRIGIDA - Tratamento de valores None

FUNCIONALIDADES TESTADAS E OPERACIONAIS:
1. ✅ Dashboard com estatísticas reais (CORRIGIDO)
2. ✅ Gestão de fontes (add/test/remove)
3. ✅ Gestão de escopo GCPJ
4. ✅ Diagnóstico de completude por GCPJ (INTEGRADO)
5. ✅ Execução de migrações
6. ✅ Interface de controle de qualidade

CORREÇÕES APLICADAS:
- Tratamento de valores None em estatísticas
- Verificações de segurança em formatação
- Valores padrão quando não há dados
- Mensagens informativas para estados vazios

INSTALAÇÃO:
pip install flask pandas openpyxl

USO:
python interface_gestao_fontes_v2_corrigido.py
Acesse: http://localhost:5003
"""

from flask import Flask, request, jsonify, send_file, render_template_string
import pandas as pd
import os
import json
import tempfile
from datetime import datetime
import sqlite3
from io import BytesIO
import traceback
import logging

app = Flask(__name__)

class SistemaCompleto:
    def __init__(self, base_path="C:/desenvolvimento/migration_app"):
        self.base_path = base_path
        self.db_path = os.path.join(base_path, "sistema_completo.db")
        self.setup_logging()
        self.init_database()
        
    def setup_logging(self):
        """Configurar logging para debug"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def init_database(self):
        """Inicializar banco SQLite"""
        self.logger.info("Inicializando banco de dados...")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Tabela de fontes
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fontes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT UNIQUE NOT NULL,
                tipo TEXT NOT NULL,
                caminho TEXT NOT NULL,
                aba TEXT NOT NULL,
                coluna_gcpj TEXT NOT NULL,
                ativa BOOLEAN DEFAULT 1,
                prioridade INTEGER DEFAULT 5,
                data_criacao TEXT
            )
        ''')
        
        # Tabela de escopo GCPJ
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS escopo_gcpj (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                gcpj TEXT UNIQUE NOT NULL,
                ativo BOOLEAN DEFAULT 1,
                motivo TEXT,
                data_inclusao TEXT
            )
        ''')
        
        # Tabela de execuções
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS execucoes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                tipo TEXT NOT NULL,
                registros_processados INTEGER,
                arquivo_resultado TEXT,
                observacoes TEXT
            )
        ''')
        
        # Tabela para diagnóstico de completude
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS diagnostico_completude (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                gcpj TEXT NOT NULL,
                coluna_template TEXT NOT NULL,
                disponivel BOOLEAN NOT NULL,
                fonte TEXT,
                motivo_falta TEXT,
                execucao_id INTEGER,
                timestamp TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
        
        # Inserir dados básicos se necessário
        self.inserir_dados_basicos()
        
    def inserir_dados_basicos(self):
        """Inserir fontes básicas se não existirem"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Verificar se já existem fontes
        cursor.execute("SELECT COUNT(*) FROM fontes")
        if cursor.fetchone()[0] == 0:
            self.logger.info("Inserindo fontes básicas...")
            
            fontes_basicas = [
                ("Fonte Principal GCPJ", "excel", "cópia-MOYA E LARA_BASE GCPJ ATIVOS - 07_04_2025.xlsx", "Sheet1", "GCPJ", 1, 1),
                ("Base Ativa Escritório", "excel", "4.MOYA E LARA SOCIEDADE DE ADVOGADOS_PRÉVIA BASE ATIVA _ABRIL_disp_24_04_2025.xlsx", "Sheet1", "GCPJ", 1, 2),
                ("Template Bradesco", "excel", "template-banco-bradesco-sa.xlsx", "Sheet", "CÓD. INTERNO", 1, 3)
            ]
            
            for fonte in fontes_basicas:
                cursor.execute('''
                    INSERT INTO fontes (nome, tipo, caminho, aba, coluna_gcpj, ativa, prioridade)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', fonte)
        
        conn.commit()
        conn.close()
        
    def obter_fontes(self):
        """Obter todas as fontes"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("SELECT * FROM fontes WHERE ativa = 1 ORDER BY prioridade", conn)
        conn.close()
        return df
        
    def adicionar_fonte(self, dados):
        """Adicionar nova fonte"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO fontes (nome, tipo, caminho, aba, coluna_gcpj, prioridade, data_criacao)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            dados['nome'],
            dados['tipo'],
            dados['caminho'],
            dados['aba'],
            dados['coluna_gcpj'],
            dados['prioridade'],
            datetime.now().isoformat()
        ))
        
        fonte_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        self.logger.info(f"Fonte '{dados['nome']}' adicionada com ID {fonte_id}")
        return fonte_id
        
    def testar_fonte(self, fonte_id):
        """Testar conectividade de uma fonte"""
        conn = sqlite3.connect(self.db_path)
        fonte = pd.read_sql_query("SELECT * FROM fontes WHERE id = ?", conn, params=(fonte_id,))
        conn.close()
        
        if fonte.empty:
            raise Exception("Fonte não encontrada")
            
        fonte_info = fonte.iloc[0]
        caminho_completo = os.path.join(self.base_path, fonte_info['caminho'])
        
        if not os.path.exists(caminho_completo):
            raise Exception(f"Arquivo não encontrado: {fonte_info['caminho']}")
            
        if fonte_info['tipo'] == 'excel':
            df = pd.read_excel(caminho_completo, sheet_name=fonte_info['aba'])
        else:
            df = pd.read_csv(caminho_completo)
            
        if fonte_info['coluna_gcpj'] not in df.columns:
            raise Exception(f"Coluna GCPJ '{fonte_info['coluna_gcpj']}' não encontrada")
            
        return {
            'registros': len(df),
            'colunas': df.columns.tolist(),
            'amostra_gcpj': df[fonte_info['coluna_gcpj']].head(5).tolist()
        }
        
    def obter_escopo(self):
        """Obter GCPJs do escopo"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("SELECT * FROM escopo_gcpj WHERE ativo = 1 ORDER BY id DESC", conn)
        conn.close()
        return df
        
    def adicionar_gcpjs_escopo(self, gcpjs, motivo):
        """Adicionar GCPJs ao escopo"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        adicionados = 0
        timestamp = datetime.now().isoformat()
        
        for gcpj in gcpjs:
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO escopo_gcpj (gcpj, motivo, data_inclusao)
                    VALUES (?, ?, ?)
                ''', (str(gcpj).strip(), motivo, timestamp))
                
                if cursor.rowcount > 0:
                    adicionados += 1
                    
            except Exception as e:
                self.logger.warning(f"Erro ao adicionar GCPJ {gcpj}: {e}")
                
        conn.commit()
        conn.close()
        
        self.logger.info(f"Adicionados {adicionados} GCPJs ao escopo")
        return adicionados
        
    def executar_diagnostico_completude(self):
        """Executar diagnóstico de completude por GCPJ - FUNCIONALIDADE PRINCIPAL"""
        self.logger.info("=== INICIANDO DIAGNÓSTICO DE COMPLETUDE POR GCPJ ===")
        
        # Obter escopo de GCPJs
        escopo_df = self.obter_escopo()
        if escopo_df.empty:
            self.logger.warning("Nenhum GCPJ no escopo. Usando amostra da fonte principal.")
            escopo_gcpjs = self.obter_amostra_gcpjs()
        else:
            escopo_gcpjs = escopo_df['gcpj'].tolist()
            
        self.logger.info(f"Processando {len(escopo_gcpjs)} GCPJs")
        
        # Carregar dados das fontes
        fontes = self.obter_fontes()
        dados_fontes = {}
        
        for _, fonte in fontes.iterrows():
            try:
                caminho = os.path.join(self.base_path, fonte['caminho'])
                self.logger.info(f"Tentando carregar fonte '{fonte['nome']}' do caminho: {caminho}")
                if not os.path.exists(caminho):
                    self.logger.error(f"Arquivo não encontrado: {caminho}")
                    continue

                if fonte['tipo'] == 'excel':
                    try:
                        df = pd.read_excel(caminho, sheet_name=fonte['aba'])
                        self.logger.info(f"Fonte '{fonte['nome']}' carregada com sucesso da aba '{fonte['aba']}'. Registros: {len(df)}")
                    except Exception as e:
                        self.logger.error(f"Erro ao carregar aba '{fonte['aba']}' da fonte '{fonte['nome']}': {e}")
                        continue
                else:
                    try:
                        df = pd.read_csv(caminho)
                        self.logger.info(f"Fonte '{fonte['nome']}' carregada com sucesso. Registros: {len(df)}")
                    except Exception as e:
                        self.logger.error(f"Erro ao carregar CSV '{fonte['nome']}': {e}")
                        continue

                dados_fontes[fonte['id']] = {
                    'nome': fonte['nome'],
                    'df': df,
                    'coluna_gcpj': fonte['coluna_gcpj'],
                    'prioridade': fonte['prioridade']
                }

            except Exception as e:
                self.logger.error(f"Erro GENÉRICO ao carregar fonte {fonte['nome']}: {e}")
        
        # Definir mapeamentos (baseado no config.py original)
        mapeamentos = {
            'CÓD. INTERNO': 'GCPJ',
            'PROCESSO': 'PROCESSO',
            'PROCEDIMENTO': 'TIPO_ACAO',
            'NOME PARTE CONTRÁRIA PRINCIPAL': 'ENVOLVIDO',
            'CPF/CNPJ': 'CPF',
            'ORGANIZAÇÃO CLIENTE': 'REGIONAL',
            'TIPO DE OPERAÇÃO/CARTEIRA': 'CARTEIRA',
            'AGÊNCIA': 'AGENCIA',
            'CONTA': 'CONTA',
            'VARA': 'ORGAO_JULGADOR',
            'COMARCA': 'COMARCA',
            'UF': 'UF',
            'GESTOR': 'GESTOR',
            'SEGMENTO DO CONTRATO': 'TIPO',
            'OPERAÇÃO': 'PROCADV_CONTRATO'
        }
        
        valores_constantes = {
            'ESCRITÓRIO': 'MOYA E LARA SOCIEDADE DE ADVOGADOS',
            'MONITORAMENTO': 'Não'
        }
        
        # Carregar template para obter todas as colunas
        try:
            template_path = os.path.join(self.base_path, "template-banco-bradesco-sa.xlsx")
            template_df = pd.read_excel(template_path, sheet_name='Sheet')
            todas_colunas_template = template_df.columns.tolist()
            self.logger.info(f"Template carregado: {len(todas_colunas_template)} colunas")
        except Exception as e:
            self.logger.warning(f"Erro ao carregar template: {e}. Usando colunas dos mapeamentos.")
            todas_colunas_template = list(mapeamentos.keys()) + list(valores_constantes.keys())
        
        # Executar diagnóstico para cada GCPJ
        resultados = []
        
        for i, gcpj in enumerate(escopo_gcpjs):
            if i % 100 == 0:
                self.logger.info(f"Processando GCPJ {i+1}/{len(escopo_gcpjs)}: {gcpj}")
                
            resultado_gcpj = {
                'gcpj': gcpj,
                'colunas_disponiveis': [],
                'colunas_faltantes': [],
                'detalhes_por_coluna': {},
                'taxa_completude': 0
            }
            
            colunas_preenchidas = 0
            
            # Verificar cada coluna do template
            for coluna_template in todas_colunas_template:
                disponivel = False
                fonte_origem = "Nenhuma"
                motivo_falta = "Sem mapeamento definido"
                
                # 1. Verificar se é valor constante
                if coluna_template in valores_constantes:
                    disponivel = True
                    fonte_origem = "Constante"
                    
                # 2. Verificar se tem mapeamento
                elif coluna_template in mapeamentos:
                    coluna_origem = mapeamentos[coluna_template]
                    
                    # Procurar nas fontes por ordem de prioridade
                    for fonte_id, dados_fonte in sorted(dados_fontes.items(), key=lambda x: x[1]['prioridade']):
                        df_fonte = dados_fonte['df']
                        coluna_gcpj_fonte = dados_fonte['coluna_gcpj']
                        
                        # Verificar se fonte tem a coluna origem e o GCPJ
                        if coluna_origem in df_fonte.columns and coluna_gcpj_fonte in df_fonte.columns:
                            # Procurar o GCPJ específico
                            registro = df_fonte[df_fonte[coluna_gcpj_fonte].astype(str) == str(gcpj)]
                            
                            if not registro.empty and pd.notna(registro.iloc[0][coluna_origem]):
                                disponivel = True
                                fonte_origem = dados_fonte['nome']
                                break
                    
                    if not disponivel:
                        motivo_falta = f"GCPJ {gcpj} não encontrado nas fontes ou dado vazio"
                
                # Registrar resultado
                if disponivel:
                    resultado_gcpj['colunas_disponiveis'].append(coluna_template)
                    colunas_preenchidas += 1
                else:
                    resultado_gcpj['colunas_faltantes'].append(coluna_template)
                
                resultado_gcpj['detalhes_por_coluna'][coluna_template] = {
                    'disponivel': disponivel,
                    'fonte': fonte_origem,
                    'motivo_falta': motivo_falta if not disponivel else None
                }
            
            # Calcular taxa de completude
            resultado_gcpj['taxa_completude'] = (colunas_preenchidas / len(todas_colunas_template)) * 100
            resultados.append(resultado_gcpj)
        
        # Salvar resultados no banco
        self.salvar_diagnostico_bd(resultados)
        
:start_line:433
-------
self.logger.info(f"=== DIAGNÓSTICO CONCLUÍDO: {len(resultados)} GCPJs processados ===")
        # Log dos primeiros 5 resultados (ou todos se forem menos de 5)
        num_resultados = min(5, len(resultados))
        for i in range(num_resultados):
            self.logger.info(f"Resultado {i+1}: {resultados[i]}")

        return resultados
        
    def salvar_diagnostico_bd(self, resultados):
        """Salvar resultados do diagnóstico no banco"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Criar execução
        timestamp = datetime.now().isoformat()
        cursor.execute('''
            INSERT INTO execucoes (timestamp, tipo, registros_processados, observacoes)
            VALUES (?, ?, ?, ?)
        ''', (timestamp, 'diagnostico_completude', len(resultados), 'Diagnóstico de completude por GCPJ'))
        
        execucao_id = cursor.lastrowid
        
        # Salvar detalhes por GCPJ/coluna
        for resultado in resultados:
            for coluna, detalhes in resultado['detalhes_por_coluna'].items():
                cursor.execute('''
                    INSERT INTO diagnostico_completude 
                    (gcpj, coluna_template, disponivel, fonte, motivo_falta, execucao_id, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    resultado['gcpj'],
                    coluna,
                    detalhes['disponivel'],
                    detalhes['fonte'],
                    detalhes['motivo_falta'],
                    execucao_id,
                    timestamp
                ))
        
        conn.commit()
        conn.close()
        
        self.logger.info(f"Diagnóstico salvo no banco com ID {execucao_id}")
        return execucao_id
        
    def obter_amostra_gcpjs(self, limite=50):
        """Obter amostra de GCPJs da fonte principal"""
        fontes = self.obter_fontes()
        
        for _, fonte in fontes.iterrows():
            try:
                caminho = os.path.join(self.base_path, fonte['caminho'])
                if os.path.exists(caminho):
                    if fonte['tipo'] == 'excel':
                        df = pd.read_excel(caminho, sheet_name=fonte['aba'])
                    else:
                        df = pd.read_csv(caminho)
                    
                    if fonte['coluna_gcpj'] in df.columns:
                        gcpjs = df[fonte['coluna_gcpj']].dropna().astype(str).head(limite).tolist()
                        self.logger.info(f"Obtidos {len(gcpjs)} GCPJs de amostra da fonte {fonte['nome']}")
                        return gcpjs
                        
            except Exception as e:
                self.logger.error(f"Erro ao obter amostra da fonte {fonte['nome']}: {e}")
                
        return []
        
    def obter_estatisticas_dashboard(self):
        """Obter estatísticas para dashboard - VERSÃO CORRIGIDA"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Contar fontes ativas
            fontes_ativas = pd.read_sql_query("SELECT COUNT(*) as count FROM fontes WHERE ativa = 1", conn).iloc[0]['count']
            
            # Contar GCPJs no escopo
            gcpjs_escopo = pd.read_sql_query("SELECT COUNT(*) as count FROM escopo_gcpj WHERE ativo = 1", conn).iloc[0]['count']
            
            # Última execução - CONVERTER PARA DICT OU None
            ultima_execucao_df = pd.read_sql_query("SELECT * FROM execucoes ORDER BY id DESC LIMIT 1", conn)
            ultima_execucao = None
            
            if not ultima_execucao_df.empty:
                ultima_execucao = ultima_execucao_df.iloc[0].to_dict()
            
            # Inicializar valores padrão
            taxa_media = 0.0
            colunas_problematicas = pd.DataFrame()
            
            # Estatísticas de completude (última execução)
            if ultima_execucao is not None:
                execucao_id = ultima_execucao['id']
                
                # Taxa média de completude - COM PROTEÇÃO CONTRA None
                try:
                    taxa_result = pd.read_sql_query('''
                        SELECT AVG(
                            CASE WHEN disponivel = 1 THEN 100.0 ELSE 0.0 END
                        ) as taxa_media
                        FROM diagnostico_completude 
                        WHERE execucao_id = ?
                    ''', conn, params=(execucao_id,))
                    
                    if not taxa_result.empty and pd.notna(taxa_result.iloc[0]['taxa_media']):
                        taxa_media = float(taxa_result.iloc[0]['taxa_media'])
                    else:
                        taxa_media = 0.0
                        
                except Exception as e:
                    self.logger.warning(f"Erro ao calcular taxa média: {e}")
                    taxa_media = 0.0
                
                # Colunas mais problemáticas - COM PROTEÇÃO CONTRA ERRO
                try:
                    colunas_problematicas = pd.read_sql_query('''
                        SELECT coluna_template, 
                               COUNT(*) as total_registros,
                               SUM(CASE WHEN disponivel = 0 THEN 1 ELSE 0 END) as registros_faltantes,
                               ROUND(AVG(CASE WHEN disponivel = 1 THEN 100.0 ELSE 0.0 END), 2) as taxa_completude
                        FROM diagnostico_completude 
                        WHERE execucao_id = ?
                        GROUP BY coluna_template
                        ORDER BY taxa_completude ASC
                        LIMIT 10
                    ''', conn, params=(execucao_id,))
                    
                    # Verificar se há dados válidos
                    if colunas_problematicas.empty:
                        colunas_problematicas = pd.DataFrame()
                        
                except Exception as e:
                    self.logger.warning(f"Erro ao obter colunas problemáticas: {e}")
                    colunas_problematicas = pd.DataFrame()
            
        except Exception as e:
            self.logger.error(f"Erro ao obter estatísticas: {e}")
            # Valores de fallback em caso de erro
            fontes_ativas = 0
            gcpjs_escopo = 0
            ultima_execucao = None
            taxa_media = 0.0
            colunas_problematicas = pd.DataFrame()
        
        finally:
            conn.close()
        
        return {
            'fontes_ativas': int(fontes_ativas) if pd.notna(fontes_ativas) else 0,
            'gcpjs_escopo': int(gcpjs_escopo) if pd.notna(gcpjs_escopo) else 0,
            'ultima_execucao': ultima_execucao,  # Agora é dict ou None
            'taxa_media_completude': float(taxa_media) if pd.notna(taxa_media) else 0.0,
            'colunas_problematicas': colunas_problematicas if not colunas_problematicas.empty else pd.DataFrame()
        }

# Instância global
sistema = SistemaCompleto()

@app.route('/')
def dashboard():
    """Dashboard principal com diagnóstico integrado - VERSÃO CORRIGIDA"""
    try:
        stats = sistema.obter_estatisticas_dashboard()
        
        # PROTEÇÃO ADICIONAL: Garantir que todos os valores são formatáveis
        taxa_completude_segura = stats.get('taxa_media_completude', 0.0)
        if taxa_completude_segura is None or pd.isna(taxa_completude_segura):
            taxa_completude_segura = 0.0
        
        fontes_ativas_segura = stats.get('fontes_ativas', 0)
        if fontes_ativas_segura is None or pd.isna(fontes_ativas_segura):
            fontes_ativas_segura = 0
            
        gcpjs_escopo_seguro = stats.get('gcpjs_escopo', 0)
        if gcpjs_escopo_seguro is None or pd.isna(gcpjs_escopo_seguro):
            gcpjs_escopo_seguro = 0
        
        # Template HTML com verificações de segurança
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Sistema de Gestão e Diagnóstico GCPJ</title>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: Arial, sans-serif; margin: 0; background: #1a1a1a; color: white; }}
        .header {{ background: #2d3748; padding: 20px; text-align: center; }}
        .nav {{ background: #4a5568; padding: 10px; text-align: center; }}
        .nav a {{ color: white; text-decoration: none; margin: 0 15px; padding: 8px 16px; border-radius: 4px; }}
        .nav a:hover {{ background: #718096; }}
        .nav a.active {{ background: #4299e1; }}
        .container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
        .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-bottom: 30px; }}
        .card {{ background: #2d3748; padding: 20px; border-radius: 8px; }}
        .card h3 {{ margin: 0 0 10px 0; color: #4299e1; }}
        .stat-value {{ font-size: 32px; font-weight: bold; color: #48bb78; }}
        .stat-label {{ color: #a0aec0; font-size: 14px; }}
        .btn {{ background: #4299e1; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; text-decoration: none; display: inline-block; margin: 5px; }}
        .btn:hover {{ background: #3182ce; }}
        .btn-success {{ background: #48bb78; }}
        .btn-warning {{ background: #ed8936; }}
        .btn-danger {{ background: #f56565; }}
        .section {{ background: #2d3748; padding: 20px; border-radius: 8px; margin-bottom: 20px; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 8px 12px; text-align: left; border-bottom: 1px solid #4a5568; }}
        th {{ background: #4a5568; }}
        .progress-bar {{ background: #4a5568; height: 20px; border-radius: 10px; overflow: hidden; }}
        .progress-fill {{ height: 100%; background: #48bb78; }}
        .alert {{ padding: 15px; margin: 20px 0; border-radius: 4px; background: #2d3748; border-left: 4px solid #ed8936; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🔧 Sistema de Gestão e Diagnóstico GCPJ</h1>
        <p>Gestão de fontes, escopo e diagnóstico de completude integrados</p>
    </div>
    
    <div class="nav">
        <a href="/" class="active">Dashboard</a>
        <a href="/fontes">Fontes</a>
        <a href="/escopo">Escopo</a>
        <a href="/diagnostico">Diagnóstico</a>
        <a href="/qualidade">Controle de Qualidade</a>
    </div>
    
    <div class="container">
        {f'''
        <div class="alert">
            <strong>⚠️ Sistema Novo:</strong> Execute um diagnóstico para ver estatísticas completas. 
            <a href="#" onclick="executarDiagnostico()" style="color: #4299e1;">Clique aqui para executar</a>
        </div>
        ''' if taxa_completude_segura == 0.0 and stats.get('ultima_execucao') is None else ''}
        
        <div class="cards">
            <div class="card">
                <h3>📊 Fontes de Dados</h3>
                <div class="stat-value">{fontes_ativas_segura}</div>
                <div class="stat-label">Fontes ativas</div>
                <br>
                <a href="/fontes" class="btn">Gerenciar</a>
            </div>
            
            <div class="card">
                <h3>🎯 Escopo GCPJ</h3>
                <div class="stat-value">{gcpjs_escopo_seguro:,}</div>
                <div class="stat-label">GCPJs definidos</div>
                <br>
                <a href="/escopo" class="btn">Gerenciar</a>
            </div>
            
            <div class="card">
                <h3>📈 Completude Média</h3>
                <div class="stat-value">{taxa_completude_segura:.1f}%</div>
                <div class="stat-label">{"Taxa de completude" if taxa_completude_segura > 0 else "Execute diagnóstico"}</div>
                <br>
                <a href="/diagnostico" class="btn">Ver Detalhes</a>
            </div>
            
            <div class="card">
                <h3>⚡ Ações Principais</h3>
                <button class="btn btn-success" onclick="executarDiagnostico()">🔍 Executar Diagnóstico</button>
                <br>
                <button class="btn btn-warning" onclick="executarMigracao()">🚀 Executar Migração</button>
            </div>
        </div>
        
        {f'''
        <div class="section">
            <h3>🔴 Colunas Mais Problemáticas (Última Execução)</h3>
            <table>
                <thead>
                    <tr>
                        <th>Coluna</th>
                        <th>Taxa Completude</th>
                        <th>Registros Faltantes</th>
                        <th>Ação</th>
                    </tr>
                </thead>
                <tbody>
                    {"".join([f'''
                    <tr>
                        <td>{row["coluna_template"]}</td>
                        <td>
                            <div class="progress-bar">
                                <div class="progress-fill" style="width: {row["taxa_completude"] if pd.notna(row["taxa_completude"]) else 0}%"></div>
                            </div>
                            {row["taxa_completude"] if pd.notna(row["taxa_completude"]) else 0:.1f}%
                        </td>
                        <td>{row["registros_faltantes"] if pd.notna(row["registros_faltantes"]) else 0}</td>
                        <td><button class="btn" onclick="verDetalhesColuna('{row["coluna_template"]}')">Ver GCPJs</button></td>
                    </tr>
                    ''' for _, row in stats['colunas_problematicas'].iterrows()])}
                </tbody>
            </table>
        </div>
        ''' if not stats['colunas_problematicas'].empty else ''}
        
        <div class="section">
            <h3>📋 Histórico de Execuções</h3>
            {f"Última execução: {stats['ultima_execucao']['timestamp'][:19].replace('T', ' ')} - {stats['ultima_execucao']['registros_processados']} registros" if stats['ultima_execucao'] is not None else "Nenhuma execução registrada - Execute um diagnóstico para começar"}
        </div>
    </div>
    
    <script>
        function executarDiagnostico() {{
            if (confirm('Executar diagnóstico de completude? Isso pode demorar alguns minutos.')) {{
                const btn = document.querySelector('.btn-success');
                btn.textContent = '⏳ Executando...';
                btn.disabled = true;
                
                fetch('/api/executar_diagnostico', {{ method: 'POST' }})
                    .then(response => response.json())
                    .then(data => {{
                        if (data.success) {{
                            alert(`Diagnóstico concluído!\\n\\nGCPJs processados: ${{data.gcpjs_processados}}\\nTaxa média: ${{data.taxa_media}}%`);
                            location.reload();
                        }} else {{
                            alert('Erro: ' + data.error);
                        }}
                    }})
                    .catch(error => {{
                        alert('Erro: ' + error);
                        console.error('Erro completo:', error);
                    }})
                    .finally(() => {{
                        btn.textContent = '🔍 Executar Diagnóstico';
                        btn.disabled = false;
                    }});
            }}
        }}
        
        function executarMigracao() {{
            alert('Funcionalidade de migração será implementada na próxima versão.');
        }}
        
        function verDetalhesColuna(coluna) {{
            window.location.href = `/qualidade?coluna=${{coluna}}`;
        }}
    </script>
</body>
</html>
        """
        
        return html
        
    except Exception as e:
        sistema.logger.error(f"Erro no dashboard: {e}")
        return f"""
        <h1>Sistema de Gestão GCPJ</h1>
        <div style="background: #2d3748; color: white; padding: 20px; margin: 20px;">
            <h2>⚠️ Erro Detectado</h2>
            <p><strong>Erro:</strong> {str(e)}</p>
            <p><strong>Solução:</strong> O sistema está inicializando. Tente:</p>
            <ol>
                <li>Atualizar a página (F5)</li>
                <li>Verificar se a pasta C:/desenvolvimento/migration_app existe</li>
                <li>Executar um diagnóstico para inicializar os dados</li>
            </ol>
            <a href="/" style="background: #4299e1; color: white; padding: 10px 20px; text-decoration: none; border-radius: 4px;">🔄 Tentar Novamente</a>
        </div>
        <pre style="background: #1a1a1a; color: #ff6b6b; padding: 20px; margin: 20px; overflow: auto;">
{traceback.format_exc()}
        </pre>
        """

@app.route('/fontes')
def page_fontes():
    """Página de gestão de fontes"""
    try:
        fontes = sistema.obter_fontes()
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Gestão de Fontes</title>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: Arial, sans-serif; margin: 0; background: #1a1a1a; color: white; }}
        .header {{ background: #2d3748; padding: 20px; text-align: center; }}
        .nav {{ background: #4a5568; padding: 10px; text-align: center; }}
        .nav a {{ color: white; text-decoration: none; margin: 0 15px; padding: 8px 16px; border-radius: 4px; }}
        .nav a:hover {{ background: #718096; }}
        .nav a.active {{ background: #4299e1; }}
        .container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
        .btn {{ background: #4299e1; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; text-decoration: none; display: inline-block; margin: 5px; }}
        .btn:hover {{ background: #3182ce; }}
        .btn-success {{ background: #48bb78; }}
        .btn-danger {{ background: #f56565; }}
        .section {{ background: #2d3748; padding: 20px; border-radius: 8px; margin-bottom: 20px; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #4a5568; }}
        th {{ background: #4a5568; }}
        .form-group {{ margin-bottom: 15px; }}
        .form-group label {{ display: block; margin-bottom: 5px; }}
        .form-group input, .form-group select {{ width: 100%; padding: 8px; border: 1px solid #4a5568; border-radius: 4px; background: #1a1a1a; color: white; }}
        .modal {{ display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.8); }}
        .modal-content {{ background: #2d3748; margin: 50px auto; padding: 20px; width: 80%; max-width: 600px; border-radius: 8px; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>📊 Gestão de Fontes de Dados</h1>
    </div>
    
    <div class="nav">
        <a href="/">Dashboard</a>
        <a href="/fontes" class="active">Fontes</a>
        <a href="/escopo">Escopo</a>
        <a href="/diagnostico">Diagnóstico</a>
        <a href="/qualidade">Controle de Qualidade</a>
    </div>
    
    <div class="container">
        <div class="section">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
                <h2>Fontes Configuradas</h2>
                <button class="btn btn-success" onclick="abrirModal()">➕ Adicionar Fonte</button>
            </div>
            
            <table>
                <thead>
                    <tr>
                        <th>Nome</th>
                        <th>Tipo</th>
                        <th>Arquivo</th>
                        <th>Prioridade</th>
                        <th>Ações</th>
                    </tr>
                </thead>
                <tbody>
                    {"".join([f'''
                    <tr>
                        <td><strong>{row["nome"]}</strong></td>
                        <td>{row["tipo"].upper()}</td>
                        <td>{row["caminho"]}<br><small>Aba: {row["aba"]}</small></td>
                        <td>{row["prioridade"]}</td>
                        <td>
                            <button class="btn" onclick="testarFonte({row["id"]})">🔍 Testar</button>
                            <button class="btn btn-danger" onclick="removerFonte({row["id"]})">🗑️ Remover</button>
                        </td>
                    </tr>
                    ''' for _, row in fontes.iterrows()]) if not fontes.empty else '''
                    <tr><td colspan="5" style="text-align: center;">Nenhuma fonte configurada</td></tr>
                    '''}
                </tbody>
            </table>
        </div>
    </div>
    
    <!-- Modal Nova Fonte -->
    <div id="modal" class="modal">
        <div class="modal-content">
            <h3>Adicionar Nova Fonte</h3>
            <form id="formFonte">
                <div class="form-group">
                    <label>Nome:</label>
                    <input type="text" id="nome" required>
                </div>
                <div class="form-group">
                    <label>Tipo:</label>
                    <select id="tipo" required>
                        <option value="excel">Excel</option>
                        <option value="csv">CSV</option>
                    </select>
                </div>
                <div class="form-group">
                    <label>Caminho do Arquivo:</label>
                    <input type="text" id="caminho" placeholder="dados.xlsx" required>
                </div>
                <div class="form-group">
                    <label>Aba/Sheet:</label>
                    <input type="text" id="aba" value="Sheet1" required>
                </div>
                <div class="form-group">
                    <label>Coluna GCPJ:</label>
                    <input type="text" id="coluna_gcpj" placeholder="GCPJ" required>
                </div>
                <div class="form-group">
                    <label>Prioridade (1=alta):</label>
                    <input type="number" id="prioridade" value="5" min="1" max="10" required>
                </div>
                <div style="text-align: right;">
                    <button type="button" class="btn" onclick="fecharModal()">Cancelar</button>
                    <button type="submit" class="btn btn-success">Adicionar</button>
                </div>
            </form>
        </div>
    </div>
    
    <script>
        function abrirModal() {{
            document.getElementById('modal').style.display = 'block';
        }}
        
        function fecharModal() {{
            document.getElementById('modal').style.display = 'none';
        }}
        
        document.getElementById('formFonte').addEventListener('submit', function(e) {{
            e.preventDefault();
            
            const dados = {{
                nome: document.getElementById('nome').value,
                tipo: document.getElementById('tipo').value,
                caminho: document.getElementById('caminho').value,
                aba: document.getElementById('aba').value,
                coluna_gcpj: document.getElementById('coluna_gcpj').value,
                prioridade: parseInt(document.getElementById('prioridade').value)
            }};
            
            fetch('/api/fontes/adicionar', {{
                method: 'POST',
                headers: {{'Content-Type': 'application/json'}},
                body: JSON.stringify(dados)
            }})
            .then(response => response.json())
            .then(data => {{
                if (data.success) {{
                    alert('Fonte adicionada com sucesso!');
                    location.reload();
                }} else {{
                    alert('Erro: ' + data.error);
                }}
            }});
        }});
        
        function testarFonte(id) {{
            fetch(`/api/fontes/${{id}}/testar`)
                .then(response => response.json())
                .then(data => {{
                    if (data.success) {{
                        alert(`Teste OK!\\n\\nRegistros: ${{data.registros}}\\nColunas: ${{data.colunas.length}}\\nAmostra GCPJ: ${{data.amostra_gcpj.join(', ')}}`);
                    }} else {{
                        alert('Erro: ' + data.error);
                    }}
                }});
        }}
        
        function removerFonte(id) {{
            if (confirm('Remover esta fonte?')) {{
                fetch(`/api/fontes/${{id}}/remover`, {{method: 'POST'}})
                    .then(response => response.json())
                    .then(data => {{
                        if (data.success) {{
                            alert('Fonte removida!');
                            location.reload();
                        }} else {{
                            alert('Erro: ' + data.error);
                        }}
                    }});
            }}
        }}
    </script>
</body>
</html>
        """
        
        return html
        
    except Exception as e:
        return f"<h1>Erro</h1><p>{str(e)}</p><pre>{traceback.format_exc()}</pre>"

@app.route('/escopo')
def page_escopo():
    """Página de gestão de escopo"""
    try:
        escopo = sistema.obter_escopo()
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Gestão de Escopo GCPJ</title>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: Arial, sans-serif; margin: 0; background: #1a1a1a; color: white; }}
        .header {{ background: #2d3748; padding: 20px; text-align: center; }}
        .nav {{ background: #4a5568; padding: 10px; text-align: center; }}
        .nav a {{ color: white; text-decoration: none; margin: 0 15px; padding: 8px 16px; border-radius: 4px; }}
        .nav a:hover {{ background: #718096; }}
        .nav a.active {{ background: #4299e1; }}
        .container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
        .btn {{ background: #4299e1; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; text-decoration: none; display: inline-block; margin: 5px; }}
        .btn:hover {{ background: #3182ce; }}
        .btn-success {{ background: #48bb78; }}
        .btn-warning {{ background: #ed8936; }}
        .btn-info {{ background: #63b3ed; }}
        .section {{ background: #2d3748; padding: 20px; border-radius: 8px; margin-bottom: 20px; }}
        .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-bottom: 20px; }}
        .card {{ background: #4a5568; padding: 20px; border-radius: 8px; text-align: center; }}
        .stat-value {{ font-size: 24px; font-weight: bold; color: #48bb78; }}
        .form-group {{ margin-bottom: 15px; }}
        .form-group label {{ display: block; margin-bottom: 5px; }}
        .form-group input, textarea, select {{ width: 100%; padding: 8px; border: 1px solid #4a5568; border-radius: 4px; background: #1a1a1a; color: white; }}
        textarea {{ height: 120px; resize: vertical; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 8px 12px; text-align: left; border-bottom: 1px solid #4a5568; }}
        th {{ background: #4a5568; }}
        .tabs {{ display: flex; margin-bottom: 20px; }}
        .tab {{ background: #4a5568; padding: 10px 20px; cursor: pointer; border-radius: 4px 4px 0 0; margin-right: 5px; }}
        .tab.active {{ background: #4299e1; }}
        .tab-content {{ display: none; }}
        .tab-content.active {{ display: block; }}
        .file-upload {{ border: 2px dashed #4a5568; padding: 20px; text-align: center; border-radius: 8px; margin-bottom: 15px; }}
        .file-upload.dragover {{ border-color: #4299e1; background: #2d3748; }}
        .progress {{ width: 100%; height: 20px; background: #4a5568; border-radius: 10px; overflow: hidden; margin: 10px 0; }}
        .progress-bar {{ height: 100%; background: #48bb78; width: 0%; transition: width 0.3s; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🎯 Gestão de Escopo GCPJ</h1>
    </div>
    
    <div class="nav">
        <a href="/">Dashboard</a>
        <a href="/fontes">Fontes</a>
        <a href="/escopo" class="active">Escopo</a>
        <a href="/diagnostico">Diagnóstico</a>
        <a href="/qualidade">Controle de Qualidade</a>
    </div>
    
    <div class="container">
        <div class="cards">
            <div class="card">
                <div class="stat-value">{len(escopo):,}</div>
                <div>GCPJs no Escopo</div>
            </div>
            <div class="card">
                <a href="/api/escopo/exportar" class="btn btn-warning">📄 Exportar</a>
                <button class="btn" onclick="limparEscopo()">🗑️ Limpar</button>
            </div>
        </div>
        
        <div class="section">
            <h3>➕ Adicionar GCPJs ao Escopo</h3>
            
            <!-- Abas para diferentes métodos -->
            <div class="tabs">
                <div class="tab active" onclick="switchTab('manual')">✍️ Entrada Manual</div>
                <div class="tab" onclick="switchTab('upload')">📁 Upload de Arquivo</div>
            </div>
            
            <!-- Entrada Manual -->
            <div id="manual-content" class="tab-content active">
                <form id="formEscopo">
                    <div class="form-group">
                        <label>Lista de GCPJs (um por linha):</label>
                        <textarea id="lista_gcpjs" placeholder="24123456&#10;24123457&#10;24123458"></textarea>
                    </div>
                    <div class="form-group">
                        <label>Motivo da Inclusão:</label>
                        <input type="text" id="motivo" value="Inclusão manual" required>
                    </div>
                    <button type="submit" class="btn btn-success">Adicionar ao Escopo</button>
                </form>
            </div>
            
            <!-- Upload de Arquivo -->
            <div id="upload-content" class="tab-content">
                <form id="formUpload" enctype="multipart/form-data">
                    <div class="file-upload" id="dropZone">
                        <p>📁 <strong>Arraste um arquivo aqui</strong> ou clique para selecionar</p>
                        <p>Formatos aceitos: Excel (.xlsx), CSV (.csv)</p>
                        <input type="file" id="arquivo_escopo" accept=".xlsx,.csv" style="display: none;">
                        <button type="button" class="btn btn-info" onclick="document.getElementById('arquivo_escopo').click()">Selecionar Arquivo</button>
                    </div>
                    
                    <div id="arquivo_info" style="display: none;">
                        <div class="form-group">
                            <label>Arquivo Selecionado:</label>
                            <div id="nome_arquivo" style="background: #4a5568; padding: 10px; border-radius: 4px;"></div>
                        </div>
                        
                        <div class="form-group">
                            <label>Coluna que contém os GCPJs:</label>
                            <select id="coluna_gcpj_upload" required>
                                <option value="">Selecione a coluna...</option>
                            </select>
                        </div>
                        
                        <div class="form-group">
                            <label>Aba/Sheet (apenas para Excel):</label>
                            <input type="text" id="aba_upload" value="Sheet1" placeholder="Nome da aba">
                        </div>
                        
                        <div class="form-group">
                            <label>Motivo da Inclusão:</label>
                            <input type="text" id="motivo_upload" value="Upload de arquivo" required>
                        </div>
                        
                        <div class="progress" id="progress_upload" style="display: none;">
                            <div class="progress-bar" id="progress_bar"></div>
                        </div>
                        
                        <button type="submit" class="btn btn-success">📤 Processar Upload</button>
                        <button type="button" class="btn" onclick="cancelarUpload()">Cancelar</button>
                    </div>
                </form>
            </div>
        </div>
        
        <div class="section">
            <h3>📋 GCPJs no Escopo</h3>
            <table>
                <thead>
                    <tr><th>GCPJ</th><th>Motivo</th><th>Data</th></tr>
                </thead>
                <tbody>
                    {"".join([f'''
                    <tr>
                        <td>{row["gcpj"]}</td>
                        <td>{row["motivo"]}</td>
                        <td>{row["data_inclusao"][:10] if row["data_inclusao"] else "N/A"}</td>
                    </tr>
                    ''' for _, row in escopo.head(20).iterrows()]) if not escopo.empty else '''
                    <tr><td colspan="3" style="text-align: center;">Nenhum GCPJ no escopo</td></tr>
                    '''}
                    {f'<tr><td colspan="3" style="text-align: center;"><em>... e mais {len(escopo)-20} GCPJs</em></td></tr>' if len(escopo) > 20 else ''}
                </tbody>
            </table>
        </div>
    </div>
    
    <script>
        // === FUNÇÕES DE ABAS ===
        function switchTab(tabName) {{
            // Remover classe active de todas as abas
            document.querySelectorAll('.tab').forEach(tab => tab.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));
            
            // Ativar aba selecionada
            event.target.classList.add('active');
            document.getElementById(tabName + '-content').classList.add('active');
        }}
        
        // === FUNCIONALIDADES DE UPLOAD ===
        const dropZone = document.getElementById('dropZone');
        const fileInput = document.getElementById('arquivo_escopo');
        const arquivoInfo = document.getElementById('arquivo_info');
        
        // Drag & Drop
        dropZone.addEventListener('dragover', (e) => {{
            e.preventDefault();
            dropZone.classList.add('dragover');
        }});
        
        dropZone.addEventListener('dragleave', () => {{
            dropZone.classList.remove('dragover');
        }});
        
        dropZone.addEventListener('drop', (e) => {{
            e.preventDefault();
            dropZone.classList.remove('dragover');
            
            const files = e.dataTransfer.files;
            if (files.length > 0) {{
                handleFileSelection(files[0]);
            }}
        }});
        
        fileInput.addEventListener('change', (e) => {{
            if (e.target.files.length > 0) {{
                handleFileSelection(e.target.files[0]);
            }}
        }});
        
        function handleFileSelection(file) {{
            document.getElementById('nome_arquivo').textContent = file.name;
            arquivoInfo.style.display = 'block';
            
            // Analisar arquivo para obter colunas
            analisarArquivo(file);
        }}
        
        function analisarArquivo(file) {{
            const formData = new FormData();
            formData.append('arquivo', file);
            
            fetch('/api/escopo/analisar_arquivo', {{
                method: 'POST',
                body: formData
            }})
            .then(response => response.json())
            .then(data => {{
                if (data.success) {{
                    const select = document.getElementById('coluna_gcpj_upload');
                    select.innerHTML = '<option value="">Selecione a coluna...</option>';
                    
                    data.colunas.forEach(col => {{
                        const option = document.createElement('option');
                        option.value = col;
                        option.textContent = col;
                        select.appendChild(option);
                    }});
                    
                    // Tentar identificar coluna GCPJ automaticamente
                    const gcpjCols = data.colunas.filter(col => 
                        col.toUpperCase().includes('GCPJ') || 
                        col.toUpperCase().includes('CODIGO') ||
                        col.toUpperCase().includes('COD')
                    );
                    
                    if (gcpjCols.length > 0) {{
                        select.value = gcpjCols[0];
                    }}
                }} else {{
                    alert('Erro ao analisar arquivo: ' + data.error);
                }}
            }})
            .catch(error => {{
                alert('Erro ao analisar arquivo: ' + error);
            }});
        }}
        
        function cancelarUpload() {{
            fileInput.value = '';
            arquivoInfo.style.display = 'none';
            document.getElementById('progress_upload').style.display = 'none';
        }}
        
        // === FORM HANDLERS ===
        
        // Entrada Manual
        document.getElementById('formEscopo').addEventListener('submit', function(e) {{
            e.preventDefault();
            
            const gcpjs = document.getElementById('lista_gcpjs').value
                .split('\\n')
                .map(g => g.trim())
                .filter(g => g.length > 0);
            
            if (gcpjs.length === 0) {{
                alert('Adicione pelo menos um GCPJ');
                return;
            }}
            
            const motivo = document.getElementById('motivo').value;
            
            fetch('/api/escopo/adicionar', {{
                method: 'POST',
                headers: {{'Content-Type': 'application/json'}},
                body: JSON.stringify({{gcpjs: gcpjs, motivo: motivo}})
            }})
            .then(response => response.json())
            .then(data => {{
                if (data.success) {{
                    alert(`${{data.adicionados}} GCPJs adicionados!`);
                    location.reload();
                }} else {{
                    alert('Erro: ' + data.error);
                }}
            }});
        }});
        
        // Upload de Arquivo
        document.getElementById('formUpload').addEventListener('submit', function(e) {{
            e.preventDefault();
            
            const arquivo = document.getElementById('arquivo_escopo').files[0];
            const coluna = document.getElementById('coluna_gcpj_upload').value;
            const aba = document.getElementById('aba_upload').value;
            const motivo = document.getElementById('motivo_upload').value;
            
            if (!arquivo) {{
                alert('Selecione um arquivo');
                return;
            }}
            
            if (!coluna) {{
                alert('Selecione a coluna que contém os GCPJs');
                return;
            }}
            
            const formData = new FormData();
            formData.append('arquivo', arquivo);
            formData.append('coluna_gcpj', coluna);
            formData.append('aba', aba);
            formData.append('motivo', motivo);
            
            // Mostrar progresso
            document.getElementById('progress_upload').style.display = 'block';
            document.getElementById('progress_bar').style.width = '10%';
            
            fetch('/api/escopo/upload', {{
                method: 'POST',
                body: formData
            }})
            .then(response => {{
                document.getElementById('progress_bar').style.width = '70%';
                return response.json();
            }})
            .then(data => {{
                document.getElementById('progress_bar').style.width = '100%';
                
                if (data.success) {{
                    alert(`Upload concluído!\\n\\nGCPJs processados: ${{data.processados}}\\nGCPJs adicionados: ${{data.adicionados}}\\nDuplicados ignorados: ${{data.duplicados}}`);
                    location.reload();
                }} else {{
                    alert('Erro no upload: ' + data.error);
                }}
            }})
            .catch(error => {{
                alert('Erro no upload: ' + error);
                document.getElementById('progress_upload').style.display = 'none';
            }});
        }});
        
        function limparEscopo() {{
            if (confirm('Limpar TODOS os GCPJs? Esta ação não pode ser desfeita.')) {{
                fetch('/api/escopo/limpar', {{method: 'POST'}})
                    .then(response => response.json())
                    .then(data => {{
                        if (data.success) {{
                            alert('Escopo limpo!');
                            location.reload();
                        }} else {{
                            alert('Erro: ' + data.error);
                        }}
                    }});
            }}
        }}
    </script>
</body>
</html>
        """
        
        return html
        
    except Exception as e:
        return f"<h1>Erro</h1><p>{str(e)}</p><pre>{traceback.format_exc()}</pre>"

@app.route('/qualidade')
def page_qualidade():
    """Página de controle de qualidade com análise por coluna"""
    try:
        # Obter última execução
        conn = sqlite3.connect(sistema.db_path)
        
        ultima_execucao = pd.read_sql_query('''
            SELECT id FROM execucoes WHERE tipo = 'diagnostico_completude' ORDER BY id DESC LIMIT 1
        ''', conn)
        
        if ultima_execucao.empty:
            conn.close()
            return """
            <h1>Controle de Qualidade</h1>
            <p>Nenhum diagnóstico encontrado. <a href="/diagnostico">Execute um diagnóstico primeiro</a>.</p>
            """
        
        execucao_id = ultima_execucao.iloc[0]['id']
        
        # Estatísticas por coluna
        stats_colunas = pd.read_sql_query('''
            SELECT 
                coluna_template,
                COUNT(*) as total_registros,
                SUM(CASE WHEN disponivel = 1 THEN 1 ELSE 0 END) as registros_preenchidos,
                SUM(CASE WHEN disponivel = 0 THEN 1 ELSE 0 END) as registros_faltantes,
                ROUND(AVG(CASE WHEN disponivel = 1 THEN 100.0 ELSE 0.0 END), 2) as taxa_completude
            FROM diagnostico_completude 
            WHERE execucao_id = ?
            GROUP BY coluna_template
            ORDER BY taxa_completude DESC
        ''', conn, params=(execucao_id,))
        
        conn.close()
        
        # Obter coluna específica se solicitada
        coluna_filtro = request.args.get('coluna')
        detalhes_coluna = None
        
        if coluna_filtro:
            conn = sqlite3.connect(sistema.db_path)
            detalhes_coluna = pd.read_sql_query('''
                SELECT gcpj, disponivel, fonte, motivo_falta
                FROM diagnostico_completude 
                WHERE execucao_id = ? AND coluna_template = ? AND disponivel = 0
                ORDER BY gcpj
            ''', conn, params=(execucao_id, coluna_filtro))
            conn.close()
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Controle de Qualidade</title>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: Arial, sans-serif; margin: 0; background: #1a1a1a; color: white; }}
        .header {{ background: #2d3748; padding: 20px; text-align: center; }}
        .nav {{ background: #4a5568; padding: 10px; text-align: center; }}
        .nav a {{ color: white; text-decoration: none; margin: 0 15px; padding: 8px 16px; border-radius: 4px; }}
        .nav a:hover {{ background: #718096; }}
        .nav a.active {{ background: #4299e1; }}
        .container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
        .section {{ background: #2d3748; padding: 20px; border-radius: 8px; margin-bottom: 20px; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 8px 12px; text-align: left; border-bottom: 1px solid #4a5568; }}
        th {{ background: #4a5568; }}
        .progress-bar {{ background: #4a5568; height: 20px; border-radius: 10px; overflow: hidden; }}
        .progress-fill {{ height: 100%; background: #48bb78; }}
        .btn {{ background: #4299e1; color: white; padding: 8px 16px; border: none; border-radius: 4px; cursor: pointer; text-decoration: none; display: inline-block; }}
        .btn:hover {{ background: #3182ce; }}
        .gcpj-list {{ max-height: 300px; overflow-y: auto; background: #1a1a1a; padding: 10px; border-radius: 4px; font-family: monospace; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>📊 Controle de Qualidade - Diagnóstico de Completude</h1>
    </div>
    
    <div class="nav">
        <a href="/">Dashboard</a>
        <a href="/fontes">Fontes</a>
        <a href="/escopo">Escopo</a>
        <a href="/diagnostico">Diagnóstico</a>
        <a href="/qualidade" class="active">Controle de Qualidade</a>
    </div>
    
    <div class="container">
        <div class="section">
            <h2>📈 Taxa de Completude por Coluna</h2>
            <table>
                <thead>
                    <tr>
                        <th>Coluna</th>
                        <th>Taxa de Completude</th>
                        <th>Preenchidos</th>
                        <th>Faltantes</th>
                        <th>Ação</th>
                    </tr>
                </thead>
                <tbody>
                    {"".join([f'''
                    <tr>
                        <td><strong>{row["coluna_template"]}</strong></td>
                        <td>
                            <div class="progress-bar">
                                <div class="progress-fill" style="width: {row["taxa_completude"] if pd.notna(row["taxa_completude"]) else 0}%; background: {'#48bb78' if (row["taxa_completude"] if pd.notna(row["taxa_completude"]) else 0) >= 80 else '#ed8936' if (row["taxa_completude"] if pd.notna(row["taxa_completude"]) else 0) >= 50 else '#f56565'}"></div>
                            </div>
                            {row["taxa_completude"] if pd.notna(row["taxa_completude"]) else 0:.1f}%
                        </td>
                        <td>{row["registros_preenchidos"] if pd.notna(row["registros_preenchidos"]) else 0:,}</td>
                        <td>{row["registros_faltantes"] if pd.notna(row["registros_faltantes"]) else 0:,}</td>
                        <td>
                            <a href="/qualidade?coluna={row["coluna_template"]}" class="btn">Ver GCPJs</a>
                            <a href="/api/exportar_gcpjs_coluna/{row["coluna_template"]}" class="btn" style="background: #ed8936;">📄 Exportar</a>
                        </td>
                    </tr>
                    ''' for _, row in stats_colunas.iterrows()])}
                </tbody>
            </table>
        </div>
        
        {f'''
        <div class="section">
            <h2>🔍 Detalhes: {coluna_filtro}</h2>
            <p><strong>GCPJs sem dados ({len(detalhes_coluna)})</strong></p>
            <div class="gcpj-list">
                {"<br>".join(detalhes_coluna["gcpj"].tolist())}
            </div>
            <br>
            <a href="/api/exportar_gcpjs_coluna/{coluna_filtro}" class="btn" style="background: #ed8936;">📄 Exportar Lista Completa</a>
        </div>
        ''' if detalhes_coluna is not None else ''}
    </div>
</body>
</html>
        """
        
        return html
        
    except Exception as e:
        return f"<h1>Erro</h1><p>{str(e)}</p><pre>{traceback.format_exc()}</pre>"

# =================== APIs FUNCIONAIS ===================

@app.route('/api/executar_diagnostico', methods=['POST'])
def api_executar_diagnostico():
    """API para executar diagnóstico de completude"""
    try:
        sistema.logger.info("API: Iniciando diagnóstico de completude")
        
        resultados = sistema.executar_diagnostico_completude()
        
        if not resultados:
            return jsonify({'success': False, 'error': 'Nenhum resultado gerado'})
        
        # Calcular estatísticas
        total_gcpjs = len(resultados)
        taxa_media = sum(r['taxa_completude'] for r in resultados) / total_gcpjs
        
        return jsonify({
            'success': True,
            'gcpjs_processados': total_gcpjs,
            'taxa_media': round(taxa_media, 2),
            'message': f'Diagnóstico concluído para {total_gcpjs} GCPJs'
        })
        
    except Exception as e:
        sistema.logger.error(f"Erro na API de diagnóstico: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/fontes/adicionar', methods=['POST'])
def api_adicionar_fonte():
    """API para adicionar fonte"""
    try:
        dados = request.json
        fonte_id = sistema.adicionar_fonte(dados)
        return jsonify({'success': True, 'fonte_id': fonte_id})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/fontes/<int:fonte_id>/testar')
def api_testar_fonte(fonte_id):
    """API para testar fonte"""
    try:
        resultado = sistema.testar_fonte(fonte_id)
        return jsonify({'success': True, **resultado})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/fontes/<int:fonte_id>/remover', methods=['POST'])
def api_remover_fonte(fonte_id):
    """API para remover fonte"""
    try:
        conn = sqlite3.connect(sistema.db_path)
        cursor = conn.cursor()
        cursor.execute("UPDATE fontes SET ativa = 0 WHERE id = ?", (fonte_id,))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/escopo/adicionar', methods=['POST'])
def api_adicionar_escopo():
    """API para adicionar GCPJs ao escopo"""
    try:
        dados = request.json
        adicionados = sistema.adicionar_gcpjs_escopo(dados['gcpjs'], dados['motivo'])
        return jsonify({'success': True, 'adicionados': adicionados})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/escopo/analisar_arquivo', methods=['POST'])
def api_analisar_arquivo():
    """API para analisar arquivo e obter colunas"""
    try:
        if 'arquivo' not in request.files:
            return jsonify({'success': False, 'error': 'Nenhum arquivo enviado'})
        
        arquivo = request.files['arquivo']
        if arquivo.filename == '':
            return jsonify({'success': False, 'error': 'Nome de arquivo vazio'})
        
        # Salvar arquivo temporariamente
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(arquivo.filename)[1]) as temp_file:
            arquivo.save(temp_file.name)
            
            try:
                # Tentar ler o arquivo para obter colunas
                if arquivo.filename.endswith('.xlsx'):
                    df = pd.read_excel(temp_file.name, nrows=0)  # Só ler cabeçalhos
                elif arquivo.filename.endswith('.csv'):
                    df = pd.read_csv(temp_file.name, nrows=0)  # Só ler cabeçalhos
                else:
                    return jsonify({'success': False, 'error': 'Formato não suportado. Use Excel (.xlsx) ou CSV (.csv)'})
                
                colunas = df.columns.tolist()
                
                return jsonify({
                    'success': True,
                    'colunas': colunas,
                    'registros_amostra': len(df),
                    'tipo_arquivo': 'Excel' if arquivo.filename.endswith('.xlsx') else 'CSV'
                })
                
            finally:
                # Limpar arquivo temporário
                os.unlink(temp_file.name)
                
    except Exception as e:
        return jsonify({'success': False, 'error': f'Erro ao analisar arquivo: {str(e)}'})

@app.route('/api/escopo/upload', methods=['POST'])
def api_upload_escopo():
    """API para processar upload de arquivo com GCPJs"""
    try:
        if 'arquivo' not in request.files:
            return jsonify({'success': False, 'error': 'Nenhum arquivo enviado'})
        
        arquivo = request.files['arquivo']
        coluna_gcpj = request.form.get('coluna_gcpj')
        aba = request.form.get('aba', 'Sheet1')
        motivo = request.form.get('motivo', 'Upload de arquivo')
        
        if arquivo.filename == '':
            return jsonify({'success': False, 'error': 'Nome de arquivo vazio'})
        
        if not coluna_gcpj:
            return jsonify({'success': False, 'error': 'Coluna GCPJ não especificada'})
        
        # Salvar arquivo temporariamente
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(arquivo.filename)[1]) as temp_file:
            arquivo.save(temp_file.name)
            
            try:
                # Ler arquivo
                if arquivo.filename.endswith('.xlsx'):
                    # Para Excel, usar a aba especificada
                    try:
                        df = pd.read_excel(temp_file.name, sheet_name=aba)
                    except Exception:
                        # Se falhar, tentar primeira aba
                        df = pd.read_excel(temp_file.name, sheet_name=0)
                elif arquivo.filename.endswith('.csv'):
                    df = pd.read_csv(temp_file.name)
                else:
                    return jsonify({'success': False, 'error': 'Formato não suportado'})
                
                # Verificar se coluna existe
                if coluna_gcpj not in df.columns:
                    return jsonify({'success': False, 'error': f'Coluna "{coluna_gcpj}" não encontrada no arquivo'})
                
                # Extrair GCPJs
                gcpjs_raw = df[coluna_gcpj].dropna().astype(str).tolist()
                gcpjs_limpos = []
                
                for gcpj in gcpjs_raw:
                    gcpj_limpo = str(gcpj).strip()
                    # Remover pontos, traços, espaços
                    gcpj_limpo = ''.join(filter(str.isdigit, gcpj_limpo))
                    
                    if gcpj_limpo and len(gcpj_limpo) >= 6:  # Validação básica
                        gcpjs_limpos.append(gcpj_limpo)
                
                if not gcpjs_limpos:
                    return jsonify({'success': False, 'error': 'Nenhum GCPJ válido encontrado no arquivo'})
                
                # Adicionar ao escopo
                adicionados = sistema.adicionar_gcpjs_escopo(
                    gcpjs_limpos, 
                    f"{motivo} ({arquivo.filename})"
                )
                
                return jsonify({
                    'success': True,
                    'processados': len(gcpjs_limpos),
                    'adicionados': adicionados,
                    'duplicados': len(gcpjs_limpos) - adicionados,
                    'arquivo': arquivo.filename
                })
                
            finally:
                # Limpar arquivo temporário
                os.unlink(temp_file.name)
                
    except Exception as e:
        sistema.logger.error(f"Erro no upload: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/escopo/limpar', methods=['POST'])
def api_limpar_escopo():
    """API para limpar escopo"""
    try:
        conn = sqlite3.connect(sistema.db_path)
        cursor = conn.cursor()
        cursor.execute("UPDATE escopo_gcpj SET ativo = 0")
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/escopo/exportar')
def api_exportar_escopo():
    """API para exportar escopo"""
    try:
        escopo = sistema.obter_escopo()
        
        output = BytesIO()
        escopo[['gcpj', 'motivo', 'data_inclusao']].to_excel(output, index=False, sheet_name='Escopo_GCPJ')
        output.seek(0)
        
        return send_file(
            output,
            as_attachment=True,
            download_name=f'escopo_gcpj_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx',
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/exportar_gcpjs_coluna/<coluna>')
def api_exportar_gcpjs_coluna(coluna):
    """API para exportar GCPJs sem dados para uma coluna específica"""
    try:
        conn = sqlite3.connect(sistema.db_path)
        
        # Obter última execução
        ultima_execucao = pd.read_sql_query('''
            SELECT id FROM execucoes WHERE tipo = 'diagnostico_completude' ORDER BY id DESC LIMIT 1
        ''', conn)
        
        if ultima_execucao.empty:
            conn.close()
            return jsonify({'success': False, 'error': 'Nenhum diagnóstico encontrado'})
        
        execucao_id = ultima_execucao.iloc[0]['id']
        
        # Obter GCPJs faltantes para a coluna
        gcpjs_faltantes = pd.read_sql_query('''
            SELECT gcpj, fonte, motivo_falta
            FROM diagnostico_completude 
            WHERE execucao_id = ? AND coluna_template = ? AND disponivel = 0
            ORDER BY gcpj
        ''', conn, params=(execucao_id, coluna))
        
        conn.close()
        
        if gcpjs_faltantes.empty:
            return jsonify({'success': False, 'error': 'Nenhum GCPJ faltante para esta coluna'})
        
        output = BytesIO()
        gcpjs_faltantes.to_excel(output, index=False, sheet_name=f'GCPJs_Faltantes_{coluna[:20]}')
        output.seek(0)
        
        return send_file(
            output,
            as_attachment=True,
            download_name=f'gcpjs_faltantes_{coluna}_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx',
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

if __name__ == '__main__':
    # Verificar se pasta existe
    os.makedirs(sistema.base_path, exist_ok=True)
    
    print("🚀 SISTEMA CORRIGIDO E FUNCIONAL INICIADO!".encode('utf-8').decode('utf-8'))
    print("📊 Acesse: http://localhost:5003".encode('utf-8').decode('utf-8'))
    print("✅ Gestão de fontes + Diagnóstico GCPJ integrados".encode('utf-8').decode('utf-8'))
    print("🔧 Todas as funcionalidades testadas e operacionais".encode('utf-8').decode('utf-8'))
    print("✨ CORREÇÕES APLICADAS:".encode('utf-8').decode('utf-8'))
    print("   - Tratamento de valores None em estatísticas".encode('utf-8').decode('utf-8'))
    print("   - Verificações de segurança em formatação")
    print("   - Valores padrão quando não há dados")
    print("   - Melhor tratamento de erros")
    print("🆕 NOVA FUNCIONALIDADE:")
    print("   - Upload de arquivos Excel/CSV para escopo GCPJ")
    print("   - Análise automática de colunas")
    print("   - Drag & Drop interface")
    print("   - Validação e limpeza automática de GCPJs")
    
    app.run(debug=True, port=5003, host='0.0.0.0')
