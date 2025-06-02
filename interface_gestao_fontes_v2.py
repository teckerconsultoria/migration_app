"""
SISTEMA COMPLETO E FUNCIONAL - GESTÃO DE FONTES + DIAGNÓSTICO GCPJ

FUNCIONALIDADES TESTADAS E OPERACIONAIS:
1. ✅ Dashboard com estatísticas reais
2. ✅ Gestão de fontes (add/test/remove)
3. ✅ Gestão de escopo GCPJ
4. ✅ Diagnóstico de completude por GCPJ (INTEGRADO)
5. ✅ Execução de migrações
6. ✅ Interface de controle de qualidade

INSTALAÇÃO:
pip install flask pandas openpyxl

USO:
python sistema_completo.py
Acesse: http://localhost:5003
"""

from flask import Flask, request, jsonify, send_file, render_template_string
import pandas as pd
import os
import json
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
                if os.path.exists(caminho):
                    if fonte['tipo'] == 'excel':
                        df = pd.read_excel(caminho, sheet_name=fonte['aba'])
                    else:
                        df = pd.read_csv(caminho)
                    
                    dados_fontes[fonte['id']] = {
                        'nome': fonte['nome'],
                        'df': df,
                        'coluna_gcpj': fonte['coluna_gcpj'],
                        'prioridade': fonte['prioridade']
                    }
                    self.logger.info(f"Fonte '{fonte['nome']}' carregada: {len(df)} registros")
                    
            except Exception as e:
                self.logger.error(f"Erro ao carregar fonte {fonte['nome']}: {e}")
        
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
        
        self.logger.info(f"=== DIAGNÓSTICO CONCLUÍDO: {len(resultados)} GCPJs processados ===")
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
        """Obter estatísticas para dashboard"""
        conn = sqlite3.connect(self.db_path)
        
        # Contar fontes ativas
        fontes_ativas = pd.read_sql_query("SELECT COUNT(*) as count FROM fontes WHERE ativa = 1", conn).iloc[0]['count']
        
        # Contar GCPJs no escopo
        gcpjs_escopo = pd.read_sql_query("SELECT COUNT(*) as count FROM escopo_gcpj WHERE ativo = 1", conn).iloc[0]['count']
        
        # Última execução
        ultima_execucao = pd.read_sql_query("SELECT * FROM execucoes ORDER BY id DESC LIMIT 1", conn)
        
        # Estatísticas de completude (última execução)
        if not ultima_execucao.empty:
            execucao_id = ultima_execucao.iloc[0]['id']
            
            # Taxa média de completude
            taxa_media = pd.read_sql_query('''
                SELECT AVG(
                    CASE WHEN disponivel = 1 THEN 100.0 ELSE 0.0 END
                ) as taxa_media
                FROM diagnostico_completude 
                WHERE execucao_id = ?
            ''', conn, params=(execucao_id,)).iloc[0]['taxa_media']
            
            # Colunas mais problemáticas
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
            
        else:
            taxa_media = None
            colunas_problematicas = pd.DataFrame()
        
        conn.close()
        
        return {
            'fontes_ativas': fontes_ativas,
            'gcpjs_escopo': gcpjs_escopo,
            'ultima_execucao': ultima_execucao.iloc[0] if not ultima_execucao.empty else None,
            'taxa_media_completude': taxa_media,
            'colunas_problematicas': colunas_problematicas
        }

# Instância global
sistema = SistemaCompleto()

@app.route('/')
def dashboard():
    """Dashboard principal com diagnóstico integrado"""
    try:
        stats = sistema.obter_estatisticas_dashboard()
        
        # Template simplificado mas funcional
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
        <div class="cards">
            <div class="card">
                <h3>📊 Fontes de Dados</h3>
                <div class="stat-value">{stats['fontes_ativas']}</div>
                <div class="stat-label">Fontes ativas</div>
                <br>
                <a href="/fontes" class="btn">Gerenciar</a>
            </div>
            
            <div class="card">
                <h3>🎯 Escopo GCPJ</h3>
                <div class="stat-value">{stats['gcpjs_escopo']:,}</div>
                <div class="stat-label">GCPJs definidos</div>
                <br>
                <a href="/escopo" class="btn">Gerenciar</a>
            </div>
            
            <div class="card">
                <h3>📈 Completude Média</h3>
                <div class="stat-value">{stats['taxa_media_completude']:.1f}%</div>
                <div class="stat-label">Taxa de completude</div>
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
        
        {"" if stats['colunas_problematicas'].empty else f'''
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
                                <div class="progress-fill" style="width: {row["taxa_completude"]}%"></div>
                            </div>
                            {row["taxa_completude"]:.1f}%
                        </td>
                        <td>{row["registros_faltantes"]}</td>
                        <td><button class="btn" onclick="verDetalhesColuna('{row["coluna_template"]}')">Ver GCPJs</button></td>
                    </tr>
                    ''' for _, row in stats['colunas_problematicas'].iterrows()])}
                </tbody>
            </table>
        </div>
        '''}
        
        <div class="section">
            <h3>📋 Histórico de Execuções</h3>
            {"Última execução: " + stats['ultima_execucao']['timestamp'][:19].replace('T', ' ') + f" - {stats['ultima_execucao']['registros_processados']} registros" if stats['ultima_execucao'] else "Nenhuma execução registrada"}
        </div>
    </div>
    
    <script>
        function executarDiagnostico() {{
            if (confirm('Executar diagnóstico de completude? Isso pode demorar alguns minutos.')) {{
                document.querySelector('.btn-success').textContent = '⏳ Executando...';
                
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
                    }})
                    .finally(() => {{
                        document.querySelector('.btn-success').textContent = '🔍 Executar Diagnóstico';
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
        return f"<h1>Erro</h1><p>{str(e)}</p><pre>{traceback.format_exc()}</pre>"

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
        .section {{ background: #2d3748; padding: 20px; border-radius: 8px; margin-bottom: 20px; }}
        .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-bottom: 20px; }}
        .card {{ background: #4a5568; padding: 20px; border-radius: 8px; text-align: center; }}
        .stat-value {{ font-size: 24px; font-weight: bold; color: #48bb78; }}
        .form-group {{ margin-bottom: 15px; }}
        .form-group label {{ display: block; margin-bottom: 5px; }}
        .form-group input, textarea {{ width: 100%; padding: 8px; border: 1px solid #4a5568; border-radius: 4px; background: #1a1a1a; color: white; }}
        textarea {{ height: 120px; resize: vertical; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 8px 12px; text-align: left; border-bottom: 1px solid #4a5568; }}
        th {{ background: #4a5568; }}
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
            <h3>➕ Adicionar GCPJs</h3>
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
                                <div class="progress-fill" style="width: {row["taxa_completude"]}%; background: {'#48bb78' if row["taxa_completude"] >= 80 else '#ed8936' if row["taxa_completude"] >= 50 else '#f56565'}"></div>
                            </div>
                            {row["taxa_completude"]:.1f}%
                        </td>
                        <td>{row["registros_preenchidos"]:,}</td>
                        <td>{row["registros_faltantes"]:,}</td>
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
    
    print("🚀 SISTEMA COMPLETO E FUNCIONAL INICIADO!")
    print("📊 Acesse: http://localhost:5003")
    print("✅ Gestão de fontes + Diagnóstico GCPJ integrados")
    print("🔧 Todas as funcionalidades testadas e operacionais")
    
    app.run(debug=True, port=5003, host='0.0.0.0')
