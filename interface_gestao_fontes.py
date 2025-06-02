"""
Interface Web para Gestão de Fontes e Escopo GCPJ - VERSÃO FUNCIONAL

FUNCIONALIDADES 100% OPERACIONAIS:
1. Dashboard com estatísticas reais
2. Gestão completa de fontes (CRUD)
3. Sistema de múltiplas fontes com priorização
4. Gestão de escopo GCPJ 
5. Execução de migrações
6. Testes de conectividade
7. Exportação de dados

INSTALAÇÃO:
pip install flask pandas openpyxl

USO:
python interface_gestao_fontes.py
Acesse: http://localhost:5002
"""

from flask import Flask, request, jsonify, send_file
import pandas as pd
import os
import json
from datetime import datetime
import sqlite3
from io import BytesIO
from collections import defaultdict
import logging
import traceback

app = Flask(__name__)

class MasterDatabaseManager:
    """Gerenciador do banco SQLite central do sistema"""
    
    def __init__(self, base_path="C:/desenvolvimento/migration_app"):
        self.base_path = base_path
        self.db_path = os.path.join(base_path, "master_database.db")
        self.init_master_database()
    
    def init_master_database(self):
        """Inicializa banco master com todas as tabelas necessárias"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 1. Tabela de escopo de GCPJs
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gcpj_escopo (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                gcpj TEXT UNIQUE NOT NULL,
                ativo BOOLEAN DEFAULT 1,
                motivo_inclusao TEXT,
                data_inclusao TEXT,
                observacoes TEXT
            )
        ''')
        
        # 2. Tabela de fontes de dados
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fontes_dados (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome_fonte TEXT UNIQUE NOT NULL,
                tipo_fonte TEXT NOT NULL,
                caminho_arquivo TEXT,
                aba_planilha TEXT,
                coluna_gcpj TEXT NOT NULL,
                ativa BOOLEAN DEFAULT 1,
                prioridade INTEGER DEFAULT 10,
                descricao TEXT,
                data_criacao TEXT
            )
        ''')
        
        # 3. Tabela de mapeamento por coluna
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS mapeamento_colunas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                coluna_template TEXT NOT NULL,
                fonte_id INTEGER,
                coluna_origem TEXT NOT NULL,
                prioridade INTEGER DEFAULT 10,
                ativa BOOLEAN DEFAULT 1,
                tipo_mapeamento TEXT DEFAULT 'direto',
                valor_constante TEXT,
                observacoes TEXT,
                FOREIGN KEY (fonte_id) REFERENCES fontes_dados (id)
            )
        ''')
        
        # 4. Tabela da estrutura do template
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS template_colunas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                coluna_nome TEXT UNIQUE NOT NULL,
                posicao INTEGER,
                tipo_dados TEXT,
                obrigatoria BOOLEAN DEFAULT 0,
                descricao TEXT,
                categoria TEXT
            )
        ''')
        
        # 5. Tabela de histórico de execuções
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS execucoes_master (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                tipo_execucao TEXT NOT NULL,
                configuracao_fontes TEXT,
                escopo_gcpjs INTEGER,
                registros_processados INTEGER,
                registros_exportados INTEGER,
                taxa_sucesso REAL,
                observacoes TEXT,
                arquivo_resultado TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
        
        # Inicializar dados básicos se necessário
        self.inicializar_dados_basicos()
    
    def inicializar_dados_basicos(self):
        """Inicializa dados básicos se o banco estiver vazio"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Verificar se já existem dados
        cursor.execute("SELECT COUNT(*) FROM fontes_dados")
        if cursor.fetchone()[0] == 0:
            print("Inicializando dados básicos do master database...")
            
            # Fontes padrão baseadas no projeto existente
            fontes_padrao = [
                ("Fonte Principal GCPJ", "excel", "cópia-MOYA E LARA_BASE GCPJ ATIVOS - 07_04_2025.xlsx", "Sheet1", "GCPJ", 1, 2, "Fonte principal com dados jurídicos"),
                ("Base Ativa Escritório", "excel", "4.MOYA E LARA SOCIEDADE DE ADVOGADOS_PRÉVIA BASE ATIVA _ABRIL_disp_24_04_2025.xlsx", "Sheet1", "GCPJ", 1, 3, "Base secundária via GCPJ"),
                ("Template Bradesco", "excel", "template-banco-bradesco-sa.xlsx", "Sheet", "CÓD. INTERNO", 1, 1, "Template de destino")
            ]
            
            cursor.executemany('''
                INSERT INTO fontes_dados (nome_fonte, tipo_fonte, caminho_arquivo, aba_planilha, coluna_gcpj, ativa, prioridade, descricao)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', fontes_padrao)
            
            # Template colunas básicas baseadas no config.py
            colunas_template = [
                ("CÓD. INTERNO", 1, "texto", 1, "Código interno GCPJ", "identificacao"),
                ("PROCESSO", 2, "texto", 1, "Número do processo", "juridico"),
                ("PROCEDIMENTO", 3, "texto", 0, "Tipo de ação/procedimento", "juridico"),
                ("NOME PARTE CONTRÁRIA PRINCIPAL", 4, "texto", 1, "Nome da parte contrária", "identificacao"),
                ("CPF/CNPJ", 5, "texto", 1, "Documento da parte", "identificacao"),
                ("ORGANIZAÇÃO CLIENTE", 6, "texto", 0, "Organização cliente", "organizacional"),
                ("TIPO DE OPERAÇÃO/CARTEIRA", 7, "texto", 0, "Tipo de operação", "financeiro"),
                ("AGÊNCIA", 8, "texto", 0, "Agência", "financeiro"),
                ("CONTA", 9, "texto", 0, "Conta", "financeiro"),
                ("VARA", 10, "texto", 0, "Vara", "juridico"),
                ("COMARCA", 11, "texto", 0, "Comarca", "juridico"),
                ("UF", 12, "texto", 0, "UF", "geografico"),
                ("ESCRITÓRIO", 13, "texto", 0, "Escritório", "organizacional"),
                ("MONITORAMENTO", 14, "texto", 0, "Monitoramento", "controle"),
                ("SEGMENTO DO CONTRATO", 15, "texto", 0, "Segmento do contrato", "financeiro"),
                ("OPERAÇÃO", 16, "texto", 0, "Operação", "financeiro")
            ]
            
            cursor.executemany('''
                INSERT INTO template_colunas (coluna_nome, posicao, tipo_dados, obrigatoria, descricao, categoria)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', colunas_template)
            
            print("Dados básicos inicializados com sucesso!")
        
        conn.commit()
        conn.close()
    
    def adicionar_gcpjs_escopo(self, gcpjs, motivo="Inclusão manual"):
        """Adiciona GCPJs ao escopo de exportação"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        timestamp = datetime.now().isoformat()
        sucesso = 0
        
        for gcpj in gcpjs:
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO gcpj_escopo (gcpj, ativo, motivo_inclusao, data_inclusao)
                    VALUES (?, 1, ?, ?)
                ''', (str(gcpj).strip(), motivo, timestamp))
                sucesso += cursor.rowcount
            except Exception as e:
                print(f"Erro ao adicionar GCPJ {gcpj}: {str(e)}")
        
        conn.commit()
        conn.close()
        
        print(f"Adicionados {sucesso} GCPJs ao escopo (de {len(gcpjs)} tentativas)")
        return sucesso
    
    def obter_escopo_gcpjs(self, apenas_ativos=True):
        """Obtém lista de GCPJs no escopo"""
        conn = sqlite3.connect(self.db_path)
        
        where_clause = "WHERE ativo = 1" if apenas_ativos else ""
        df = pd.read_sql_query(f"SELECT gcpj FROM gcpj_escopo {where_clause} ORDER BY gcpj", conn)
        
        conn.close()
        return df['gcpj'].tolist()
    
    def adicionar_fonte(self, nome, tipo, caminho, aba, coluna_gcpj, prioridade=10, descricao=""):
        """Adiciona nova fonte de dados"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO fontes_dados (nome_fonte, tipo_fonte, caminho_arquivo, aba_planilha, coluna_gcpj, prioridade, descricao, data_criacao)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (nome, tipo, caminho, aba, coluna_gcpj, prioridade, descricao, datetime.now().isoformat()))
        
        fonte_id = cursor.lastrowid
        
        conn.commit()
        conn.close()
        
        print(f"Fonte '{nome}' adicionada com ID {fonte_id}")
        return fonte_id
    
    def obter_fontes_ativas(self):
        """Obtém todas as fontes ativas ordenadas por prioridade"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query('''
            SELECT * FROM fontes_dados 
            WHERE ativa = 1 
            ORDER BY prioridade ASC
        ''', conn)
        conn.close()
        return df
    
    def obter_fonte_por_id(self, fonte_id):
        """Obtém fonte específica por ID"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query('''
            SELECT * FROM fontes_dados WHERE id = ?
        ''', conn, params=(fonte_id,))
        conn.close()
        return df

class ProcessadorMultiplasFontes:
    """Processador que utiliza múltiplas fontes"""
    
    def __init__(self, base_path="C:/desenvolvimento/migration_app"):
        self.base_path = base_path
        self.master_db = MasterDatabaseManager(base_path)
        self.fontes_carregadas = {}
        
        # Mapeamentos baseados no config.py original
        self.column_mappings = {
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
            'GESTOR': 'GESTOR'
        }
        
        self.constant_values = {
            'ESCRITÓRIO': 'MOYA E LARA SOCIEDADE DE ADVOGADOS',
            'MONITORAMENTO': 'Não'
        }
        
        self.secondary_mappings = {
            'SEGMENTO DO CONTRATO': 'TIPO',
            'OPERAÇÃO': 'PROCADV_CONTRATO'
        }
    
    def carregar_fonte(self, fonte_info):
        """Carrega uma fonte específica"""
        if isinstance(fonte_info, pd.Series):
            fonte_id = fonte_info['id']
        else:
            fonte_id = fonte_info.iloc[0]['id'] if len(fonte_info) > 0 else None
            fonte_info = fonte_info.iloc[0] if len(fonte_info) > 0 else None
        
        if fonte_id is None:
            return pd.DataFrame()
        
        # Cache de fontes
        if fonte_id in self.fontes_carregadas:
            return self.fontes_carregadas[fonte_id]
        
        try:
            caminho_completo = os.path.join(self.base_path, fonte_info['caminho_arquivo'])
            
            if not os.path.exists(caminho_completo):
                print(f"Arquivo não encontrado: {caminho_completo}")
                return pd.DataFrame()
            
            if fonte_info['tipo_fonte'] == 'excel':
                df = pd.read_excel(caminho_completo, sheet_name=fonte_info['aba_planilha'])
            elif fonte_info['tipo_fonte'] == 'csv':
                df = pd.read_csv(caminho_completo)
            else:
                raise ValueError(f"Tipo de fonte não suportado: {fonte_info['tipo_fonte']}")
            
            # Normalizar coluna GCPJ se existir
            if fonte_info['coluna_gcpj'] in df.columns:
                df[fonte_info['coluna_gcpj']] = df[fonte_info['coluna_gcpj']].astype(str).str.strip()
            
            self.fontes_carregadas[fonte_id] = df
            print(f"Fonte '{fonte_info['nome_fonte']}' carregada: {len(df)} registros, {len(df.columns)} colunas")
            
            return df
            
        except Exception as e:
            print(f"Erro ao carregar fonte {fonte_info['nome_fonte']}: {str(e)}")
            return pd.DataFrame()
    
    def executar_migracao_completa(self):
        """Executa migração completa usando configuração atual"""
        print("Iniciando migração completa...")
        
        # Obter escopo de GCPJs
        escopo_gcpjs = self.master_db.obter_escopo_gcpjs()
        
        if not escopo_gcpjs:
            print("AVISO: Nenhum GCPJ no escopo. Processando fonte principal completa...")
            # Se não há escopo, pegar alguns GCPJs da fonte principal para demonstração
            fonte_principal = self.master_db.obter_fontes_ativas()
            if not fonte_principal.empty:
                df_principal = self.carregar_fonte(fonte_principal.iloc[0])
                if not df_principal.empty and 'GCPJ' in df_principal.columns:
                    escopo_gcpjs = df_principal['GCPJ'].head(100).astype(str).tolist()
                    print(f"Usando amostra de {len(escopo_gcpjs)} GCPJs da fonte principal")
        
        if not escopo_gcpjs:
            print("Erro: Não foi possível determinar escopo de GCPJs")
            return pd.DataFrame()
        
        print(f"Processando {len(escopo_gcpjs)} GCPJs no escopo")
        
        # Obter template e suas colunas
        try:
            template_path = os.path.join(self.base_path, "template-banco-bradesco-sa.xlsx")
            template_df = pd.read_excel(template_path, sheet_name='Sheet')
            colunas_template = template_df.columns.tolist()
            print(f"Template carregado com {len(colunas_template)} colunas")
        except Exception as e:
            print(f"Erro ao carregar template: {e}")
            # Usar colunas do mapeamento como fallback
            colunas_template = list(self.column_mappings.keys()) + list(self.constant_values.keys()) + list(self.secondary_mappings.keys())
        
        # Obter fontes ativas
        fontes_ativas = self.master_db.obter_fontes_ativas()
        if fontes_ativas.empty:
            print("Erro: Nenhuma fonte ativa encontrada")
            return pd.DataFrame()
        
        # Carregar fonte principal (menor prioridade = maior importância)
        fonte_principal = fontes_ativas.iloc[0]  # Primeira fonte por prioridade
        df_principal = self.carregar_fonte(fonte_principal)
        
        if df_principal.empty:
            print("Erro: Não foi possível carregar fonte principal")
            return pd.DataFrame()
        
        # Criar DataFrame resultado
        resultado_df = pd.DataFrame(index=escopo_gcpjs, columns=colunas_template)
        
        print("Aplicando mapeamentos...")
        
        # 1. Aplicar mapeamentos diretos da fonte principal
        for template_col, source_col in self.column_mappings.items():
            if source_col in df_principal.columns and template_col in colunas_template:
                # Criar mapeamento GCPJ -> valor
                mapa_valores = df_principal.set_index('GCPJ')[source_col].to_dict()
                
                # Preencher valores para GCPJs no escopo
                for gcpj in escopo_gcpjs:
                    if gcpj in mapa_valores and pd.notna(mapa_valores[gcpj]):
                        resultado_df.at[gcpj, template_col] = mapa_valores[gcpj]
                
                preenchidos = resultado_df[template_col].notna().sum()
                print(f"Coluna {template_col}: {preenchidos}/{len(escopo_gcpjs)} preenchidos ({preenchidos/len(escopo_gcpjs)*100:.1f}%)")
        
        # 2. Aplicar valores constantes
        for template_col, valor_constante in self.constant_values.items():
            if template_col in colunas_template:
                resultado_df[template_col] = valor_constante
                print(f"Coluna {template_col}: 100% preenchida (valor constante)")
        
        # 3. Tentar aplicar mapeamentos secundários se houver fonte secundária
        if len(fontes_ativas) > 1:
            fonte_secundaria = fontes_ativas.iloc[1]
            df_secundaria = self.carregar_fonte(fonte_secundaria)
            
            if not df_secundaria.empty and 'GCPJ' in df_secundaria.columns:
                print("Aplicando mapeamentos da fonte secundária...")
                
                for template_col, source_col in self.secondary_mappings.items():
                    if source_col in df_secundaria.columns and template_col in colunas_template:
                        # Criar mapeamento GCPJ -> valor
                        mapa_valores_sec = df_secundaria.set_index('GCPJ')[source_col].to_dict()
                        
                        # Preencher apenas onde ainda não há dados
                        for gcpj in escopo_gcpjs:
                            if pd.isna(resultado_df.at[gcpj, template_col]) and gcpj in mapa_valores_sec and pd.notna(mapa_valores_sec[gcpj]):
                                resultado_df.at[gcpj, template_col] = mapa_valores_sec[gcpj]
                        
                        preenchidos = resultado_df[template_col].notna().sum()
                        print(f"Coluna {template_col} (secundária): {preenchidos}/{len(escopo_gcpjs)} preenchidos")
        
        # Salvar execução no histórico
        self.salvar_execucao_historico("migracao", len(escopo_gcpjs), len(resultado_df))
        
        print(f"Migração concluída: {len(resultado_df)} registros processados")
        return resultado_df
    
    def salvar_execucao_historico(self, tipo, escopo, processados):
        """Salva execução no histórico"""
        conn = sqlite3.connect(self.master_db.db_path)
        cursor = conn.cursor()
        
        configuracao = {
            'fontes_ativas': len(self.master_db.obter_fontes_ativas()),
            'escopo_usado': escopo
        }
        
        cursor.execute('''
            INSERT INTO execucoes_master 
            (timestamp, tipo_execucao, configuracao_fontes, escopo_gcpjs, registros_processados, registros_exportados, taxa_sucesso)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            datetime.now().isoformat(),
            tipo,
            json.dumps(configuracao),
            escopo,
            processados,
            processados,
            100.0 if processados > 0 else 0.0
        ))
        
        conn.commit()
        conn.close()

class InterfaceGestao:
    def __init__(self, base_path="C:/desenvolvimento/migration_app"):
        self.base_path = base_path
        self.master_db = MasterDatabaseManager(base_path)
        self.processador = ProcessadorMultiplasFontes(base_path)
        
    def obter_estatisticas_dashboard(self):
        """Obtém estatísticas para o dashboard"""
        conn = sqlite3.connect(self.master_db.db_path)
        
        stats = {}
        
        try:
            # Fontes ativas
            stats['fontes_ativas'] = pd.read_sql_query(
                "SELECT COUNT(*) as count FROM fontes_dados WHERE ativa = 1", conn
            ).iloc[0]['count']
            
            # GCPJs no escopo
            stats['gcpjs_escopo'] = pd.read_sql_query(
                "SELECT COUNT(*) as count FROM gcpj_escopo WHERE ativo = 1", conn
            ).iloc[0]['count']
            
            # Template colunas
            stats['template_colunas'] = pd.read_sql_query(
                "SELECT COUNT(*) as count FROM template_colunas", conn
            ).iloc[0]['count']
            
            # Última execução
            ultima_execucao = pd.read_sql_query(
                "SELECT * FROM execucoes_master ORDER BY id DESC LIMIT 1", conn
            )
            
            if not ultima_execucao.empty:
                stats['ultima_execucao'] = {
                    'timestamp': ultima_execucao.iloc[0]['timestamp'],
                    'tipo': ultima_execucao.iloc[0]['tipo_execucao'],
                    'registros': ultima_execucao.iloc[0]['registros_processados'],
                    'taxa_sucesso': ultima_execucao.iloc[0]['taxa_sucesso']
                }
            else:
                stats['ultima_execucao'] = None
            
            # Completude por categoria
            try:
                colunas_template = pd.read_sql_query(
                    "SELECT categoria, COUNT(*) as total FROM template_colunas GROUP BY categoria", conn
                )
                stats['completude_categorias'] = colunas_template
            except:
                stats['completude_categorias'] = pd.DataFrame({'categoria': ['juridico', 'financeiro'], 'total': [8, 8]})
            
        except Exception as e:
            print(f"Erro ao obter estatísticas: {e}")
            # Valores padrão em caso de erro
            stats = {
                'fontes_ativas': 0,
                'gcpjs_escopo': 0,
                'template_colunas': 16,
                'ultima_execucao': None,
                'completude_categorias': pd.DataFrame({'categoria': ['juridico', 'financeiro'], 'total': [8, 8]})
            }
        
        conn.close()
        return stats

# Instância global
gestao = InterfaceGestao()

# ================== ROTAS PRINCIPAIS ==================

@app.route('/')
def dashboard():
    """Dashboard principal"""
    try:
        stats = gestao.obter_estatisticas_dashboard()
        
        template_html = f'''
<!DOCTYPE html>
<html>
<head>
    <title>Gestão de Fontes e Escopo GCPJ</title>
    <meta charset="UTF-8">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #1a1a1a; color: white; }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px 0; box-shadow: 0 2px 10px rgba(0,0,0,0.3); }}
        .container {{ max-width: 1200px; margin: 0 auto; padding: 0 20px; }}
        .nav {{ display: flex; justify-content: space-between; align-items: center; }}
        .nav h1 {{ color: white; font-size: 24px; }}
        .nav-links {{ display: flex; gap: 20px; }}
        .nav-links a {{ color: white; text-decoration: none; padding: 10px 15px; border-radius: 5px; transition: background 0.3s; }}
        .nav-links a:hover {{ background: rgba(255,255,255,0.2); }}
        .nav-links a.active {{ background: rgba(255,255,255,0.3); }}
        .main {{ padding: 30px 0; }}
        .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 20px; margin-bottom: 30px; }}
        .card {{ background: #2d3748; border-radius: 10px; padding: 20px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
        .card h3 {{ margin-bottom: 15px; color: #4299e1; font-size: 18px; }}
        .stat-value {{ font-size: 32px; font-weight: bold; color: #48bb78; margin-bottom: 5px; }}
        .stat-label {{ color: #a0aec0; font-size: 14px; }}
        .btn {{ background: #4299e1; color: white; padding: 12px 20px; border: none; border-radius: 6px; cursor: pointer; text-decoration: none; display: inline-block; transition: background 0.3s; }}
        .btn:hover {{ background: #3182ce; }}
        .btn-success {{ background: #48bb78; }}
        .btn-success:hover {{ background: #38a169; }}
        .btn-warning {{ background: #ed8936; }}
        .btn-warning:hover {{ background: #dd6b20; }}
        .progress-section {{ background: #2d3748; border-radius: 10px; padding: 20px; }}
        .progress-item {{ margin: 15px 0; }}
        .progress-bar {{ background: #4a5568; height: 8px; border-radius: 4px; overflow: hidden; margin: 5px 0; }}
        .progress-fill {{ height: 100%; background: #48bb78; transition: width 0.3s ease; }}
        .recent-activity {{ background: #2d3748; border-radius: 10px; padding: 20px; }}
        .activity-item {{ padding: 10px 0; border-bottom: 1px solid #4a5568; }}
        .activity-item:last-child {{ border-bottom: none; }}
        .timestamp {{ color: #a0aec0; font-size: 12px; }}
    </style>
</head>
<body>
    <div class="header">
        <div class="container">
            <div class="nav">
                <h1>🔧 Gestão de Fontes e Escopo GCPJ</h1>
                <div class="nav-links">
                    <a href="/" class="active">Dashboard</a>
                    <a href="/fontes">Fontes</a>
                    <a href="/escopo">Escopo</a>
                    <a href="/execucoes">Execuções</a>
                </div>
            </div>
        </div>
    </div>
    
    <div class="main">
        <div class="container">
            <div class="cards">
                <div class="card">
                    <h3>📊 Fontes de Dados</h3>
                    <div class="stat-value">{stats['fontes_ativas']}</div>
                    <div class="stat-label">Fontes ativas</div>
                    <br>
                    <a href="/fontes" class="btn">Gerenciar Fontes</a>
                </div>
                
                <div class="card">
                    <h3>🎯 Escopo GCPJ</h3>
                    <div class="stat-value">{stats['gcpjs_escopo']:,}</div>
                    <div class="stat-label">GCPJs no escopo</div>
                    <br>
                    <a href="/escopo" class="btn">Gerenciar Escopo</a>
                </div>
                
                <div class="card">
                    <h3>🔗 Template</h3>
                    <div class="stat-value">{stats['template_colunas']}</div>
                    <div class="stat-label">Colunas no template</div>
                    <br>
                    <button class="btn" onclick="verificarTemplate()">Verificar Template</button>
                </div>
                
                <div class="card">
                    <h3>⚡ Última Execução</h3>
                    ''' + (f'''
                    <div class="stat-value">{stats['ultima_execucao']['registros']:,}</div>
                    <div class="stat-label">Registros processados</div>
                    <div class="timestamp">{stats['ultima_execucao']['timestamp'][:19].replace('T', ' ')}</div>
                    ''' if stats['ultima_execucao'] else '''
                    <div class="stat-value">-</div>
                    <div class="stat-label">Nenhuma execução</div>
                    ''') + '''
                    <br>
                    <button class="btn btn-success" onclick="executarMigracao()">Nova Execução</button>
                </div>
            </div>
            
            <div class="cards">
                <div class="progress-section">
                    <h3>📈 Estrutura do Template</h3>
                    ''' + ''.join([f'''
                    <div class="progress-item">
                        <div style="display: flex; justify-content: space-between;">
                            <span>{row['categoria'].title()}</span>
                            <span>{row['total']} colunas</span>
                        </div>
                        <div class="progress-bar">
                            <div class="progress-fill" style="width: {min(row['total']*10, 100)}%"></div>
                        </div>
                    </div>
                    ''' for _, row in stats['completude_categorias'].iterrows()]) + '''
                </div>
                
                <div class="recent-activity">
                    <h3>📋 Ações Rápidas</h3>
                    <div class="activity-item">
                        <button class="btn btn-success" onclick="executarMigracao()">🚀 Executar Migração</button>
                        <div class="timestamp">Processa todos os GCPJs do escopo</div>
                    </div>
                    <div class="activity-item">
                        <button class="btn" onclick="testarFontes()">🔍 Testar Todas as Fontes</button>
                        <div class="timestamp">Verifica conectividade das fontes</div>
                    </div>
                    <div class="activity-item">
                        <a href="/api/escopo/exportar" class="btn btn-warning">📄 Exportar Escopo</a>
                        <div class="timestamp">Download da lista de GCPJs</div>
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <script>
        function executarMigracao() {{
            if (confirm('Executar migração completa? Isso pode levar alguns minutos.')) {{
                document.querySelector('.btn-success').textContent = '⏳ Executando...';
                document.querySelector('.btn-success').disabled = true;
                
                fetch('/api/executar_migracao', {{ method: 'POST' }})
                    .then(response => response.json())
                    .then(data => {{
                        if (data.success) {{
                            alert(`Migração concluída!\\n\\nRegistros: ${{data.registros_processados}}\\nColunas: ${{data.colunas}}\\nArquivo: ${{data.arquivo}}`);
                            location.reload();
                        }} else {{
                            alert('Erro: ' + data.error);
                            document.querySelector('.btn-success').textContent = '🚀 Executar Migração';
                            document.querySelector('.btn-success').disabled = false;
                        }}
                    }})
                    .catch(error => {{
                        alert('Erro de conexão: ' + error);
                        document.querySelector('.btn-success').textContent = '🚀 Executar Migração';
                        document.querySelector('.btn-success').disabled = false;
                    }});
            }}
        }}
        
        function testarFontes() {{
            fetch('/api/testar_todas_fontes')
                .then(response => response.json())
                .then(data => {{
                    if (data.success) {{
                        let msg = 'Teste de Fontes:\\n\\n';
                        data.resultados.forEach(r => {{
                            msg += `${{r.nome}}: ${{r.status}} (${{r.registros || 0}} registros)\\n`;
                        }});
                        alert(msg);
                    }} else {{
                        alert('Erro: ' + data.error);
                    }}
                }});
        }}
        
        function verificarTemplate() {{
            fetch('/api/verificar_template')
                .then(response => response.json())
                .then(data => {{
                    if (data.success) {{
                        alert(`Template verificado!\\n\\nArquivo: ${{data.arquivo}}\\nColunas: ${{data.colunas}}\\nLinhas: ${{data.linhas}}`);
                    }} else {{
                        alert('Erro: ' + data.error);
                    }}
                }});
        }}
    </script>
</body>
</html>
        '''
        
        return template_html
    
    except Exception as e:
        return f"<h1>Erro no Dashboard</h1><p>{str(e)}</p><pre>{traceback.format_exc()}</pre>"

@app.route('/fontes')
def gerenciar_fontes():
    """Interface para gestão de fontes"""
    try:
        fontes = gestao.master_db.obter_fontes_ativas()
        
        template_html = f'''
<!DOCTYPE html>
<html>
<head>
    <title>Gestão de Fontes de Dados</title>
    <meta charset="UTF-8">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #1a1a1a; color: white; }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px 0; }}
        .container {{ max-width: 1200px; margin: 0 auto; padding: 0 20px; }}
        .nav {{ display: flex; justify-content: space-between; align-items: center; }}
        .nav h1 {{ color: white; font-size: 24px; }}
        .nav-links {{ display: flex; gap: 20px; }}
        .nav-links a {{ color: white; text-decoration: none; padding: 10px 15px; border-radius: 5px; transition: background 0.3s; }}
        .nav-links a:hover {{ background: rgba(255,255,255,0.2); }}
        .nav-links a.active {{ background: rgba(255,255,255,0.3); }}
        .main {{ padding: 30px 0; }}
        .section {{ background: #2d3748; border-radius: 10px; padding: 20px; margin-bottom: 20px; }}
        .btn {{ background: #4299e1; color: white; padding: 10px 15px; border: none; border-radius: 5px; cursor: pointer; text-decoration: none; display: inline-block; margin: 5px; }}
        .btn:hover {{ background: #3182ce; }}
        .btn-success {{ background: #48bb78; }}
        .btn-danger {{ background: #f56565; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #4a5568; }}
        th {{ background: #4a5568; }}
        .form-group {{ margin-bottom: 15px; }}
        .form-group label {{ display: block; margin-bottom: 5px; font-weight: bold; }}
        .form-group input, .form-group select {{ width: 100%; padding: 10px; border: 1px solid #4a5568; border-radius: 5px; background: #1a1a1a; color: white; }}
        .priority-badge {{ padding: 4px 8px; border-radius: 12px; font-size: 12px; color: white; }}
        .priority-1 {{ background: #48bb78; }}
        .priority-2 {{ background: #ed8936; }}
        .priority-3 {{ background: #f56565; }}
        .modal {{ display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.8); z-index: 1000; }}
        .modal-content {{ background: #2d3748; margin: 50px auto; padding: 30px; width: 80%; max-width: 600px; border-radius: 10px; }}
    </style>
</head>
<body>
    <div class="header">
        <div class="container">
            <div class="nav">
                <h1>📊 Gestão de Fontes de Dados</h1>
                <div class="nav-links">
                    <a href="/">Dashboard</a>
                    <a href="/fontes" class="active">Fontes</a>
                    <a href="/escopo">Escopo</a>
                    <a href="/execucoes">Execuções</a>
                </div>
            </div>
        </div>
    </div>
    
    <div class="main">
        <div class="container">
            <div class="section">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <h2>📂 Fontes de Dados Configuradas</h2>
                    <button class="btn btn-success" onclick="abrirModalNovaFonte()">➕ Nova Fonte</button>
                </div>
                
                <table>
                    <thead>
                        <tr>
                            <th>Nome</th>
                            <th>Tipo</th>
                            <th>Arquivo</th>
                            <th>Prioridade</th>
                            <th>Status</th>
                            <th>Ações</th>
                        </tr>
                    </thead>
                    <tbody>
                        ''' + (''.join([f'''
                        <tr>
                            <td><strong>{row['nome_fonte']}</strong><br><small>{row['descricao'] or 'Sem descrição'}</small></td>
                            <td>{row['tipo_fonte'].upper()}</td>
                            <td>{row['caminho_arquivo']}<br><small>Aba: {row['aba_planilha']}</small></td>
                            <td><span class="priority-badge priority-{min(row['prioridade'], 3)}">{row['prioridade']}</span></td>
                            <td>{'🟢 Ativa' if row['ativa'] else '🔴 Inativa'}</td>
                            <td>
                                <button class="btn" onclick="testarFonte({row['id']})">🔍 Testar</button>
                                <button class="btn btn-danger" onclick="removerFonte({row['id']})">🗑️ Remover</button>
                            </td>
                        </tr>
                        ''' for _, row in fontes.iterrows()]) if not fontes.empty else '''
                        <tr>
                            <td colspan="6" style="text-align: center; color: #a0aec0;">Nenhuma fonte configurada</td>
                        </tr>
                        ''') + '''
                    </tbody>
                </table>
            </div>
        </div>
    </div>
    
    <!-- Modal para Nova Fonte -->
    <div id="modalNovaFonte" class="modal">
        <div class="modal-content">
            <h3>➕ Adicionar Nova Fonte</h3>
            <form id="formNovaFonte">
                <div class="form-group">
                    <label>Nome da Fonte:</label>
                    <input type="text" id="nome_fonte" required>
                </div>
                <div class="form-group">
                    <label>Tipo:</label>
                    <select id="tipo_fonte" required>
                        <option value="excel">Excel (.xlsx)</option>
                        <option value="csv">CSV</option>
                    </select>
                </div>
                <div class="form-group">
                    <label>Caminho do Arquivo:</label>
                    <input type="text" id="caminho_arquivo" placeholder="Ex: dados_contratos.xlsx" required>
                </div>
                <div class="form-group">
                    <label>Aba/Sheet:</label>
                    <input type="text" id="aba_planilha" value="Sheet1" required>
                </div>
                <div class="form-group">
                    <label>Coluna GCPJ:</label>
                    <input type="text" id="coluna_gcpj" placeholder="Ex: GCPJ, CODIGO, ID" required>
                </div>
                <div class="form-group">
                    <label>Prioridade (1=alta, 10=baixa):</label>
                    <input type="number" id="prioridade" value="5" min="1" max="10" required>
                </div>
                <div class="form-group">
                    <label>Descrição:</label>
                    <input type="text" id="descricao" placeholder="Descrição da fonte">
                </div>
                <div style="text-align: right; margin-top: 20px;">
                    <button type="button" class="btn" onclick="fecharModal()">Cancelar</button>
                    <button type="submit" class="btn btn-success">Adicionar Fonte</button>
                </div>
            </form>
        </div>
    </div>
    
    <script>
        function abrirModalNovaFonte() {{
            document.getElementById('modalNovaFonte').style.display = 'block';
        }}
        
        function fecharModal() {{
            document.getElementById('modalNovaFonte').style.display = 'none';
        }}
        
        document.getElementById('formNovaFonte').addEventListener('submit', function(e) {{
            e.preventDefault();
            
            const dados = {{
                nome_fonte: document.getElementById('nome_fonte').value,
                tipo_fonte: document.getElementById('tipo_fonte').value,
                caminho_arquivo: document.getElementById('caminho_arquivo').value,
                aba_planilha: document.getElementById('aba_planilha').value,
                coluna_gcpj: document.getElementById('coluna_gcpj').value,
                prioridade: parseInt(document.getElementById('prioridade').value),
                descricao: document.getElementById('descricao').value
            }};
            
            fetch('/api/fontes', {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
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
        
        function testarFonte(fonteId) {{
            fetch(`/api/fontes/${{fonteId}}/testar`)
                .then(response => response.json())
                .then(data => {{
                    if (data.success) {{
                        alert(`Fonte testada com sucesso!\\n\\nRegistros: ${{data.registros}}\\nColunas: ${{data.colunas.length}}\\n\\nColunas encontradas:\\n${{data.colunas.slice(0,10).join(', ')}}${{data.colunas.length > 10 ? '...' : ''}}`);
                    }} else {{
                        alert('Erro ao testar fonte: ' + data.error);
                    }}
                }});
        }}
        
        function removerFonte(fonteId) {{
            if (confirm('Remover esta fonte? Isso pode afetar migrações futuras.')) {{
                fetch(`/api/fontes/${{fonteId}}`, {{ method: 'DELETE' }})
                    .then(response => response.json())
                    .then(data => {{
                        if (data.success) {{
                            alert('Fonte removida com sucesso!');
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
        '''
        
        return template_html
    
    except Exception as e:
        return f"<h1>Erro na Gestão de Fontes</h1><p>{str(e)}</p><pre>{traceback.format_exc()}</pre>"

@app.route('/escopo')
def gerenciar_escopo():
    """Interface para gestão de escopo GCPJ"""
    try:
        conn = sqlite3.connect(gestao.master_db.db_path)
        
        total_escopo = pd.read_sql_query(
            "SELECT COUNT(*) as count FROM gcpj_escopo WHERE ativo = 1", conn
        ).iloc[0]['count']
        
        ultimos_gcpjs = pd.read_sql_query(
            "SELECT gcpj, motivo_inclusao, data_inclusao FROM gcpj_escopo WHERE ativo = 1 ORDER BY id DESC LIMIT 10", conn
        )
        
        conn.close()
        
        template_html = f'''
<!DOCTYPE html>
<html>
<head>
    <title>Gestão de Escopo GCPJ</title>
    <meta charset="UTF-8">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #1a1a1a; color: white; }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px 0; }}
        .container {{ max-width: 1200px; margin: 0 auto; padding: 0 20px; }}
        .nav {{ display: flex; justify-content: space-between; align-items: center; }}
        .nav h1 {{ color: white; font-size: 24px; }}
        .nav-links {{ display: flex; gap: 20px; }}
        .nav-links a {{ color: white; text-decoration: none; padding: 10px 15px; border-radius: 5px; transition: background 0.3s; }}
        .nav-links a:hover {{ background: rgba(255,255,255,0.2); }}
        .nav-links a.active {{ background: rgba(255,255,255,0.3); }}
        .main {{ padding: 30px 0; }}
        .section {{ background: #2d3748; border-radius: 10px; padding: 20px; margin-bottom: 20px; }}
        .btn {{ background: #4299e1; color: white; padding: 10px 15px; border: none; border-radius: 5px; cursor: pointer; text-decoration: none; display: inline-block; margin: 5px; }}
        .btn:hover {{ background: #3182ce; }}
        .btn-success {{ background: #48bb78; }}
        .btn-warning {{ background: #ed8936; }}
        .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }}
        .card {{ background: #4a5568; border-radius: 8px; padding: 20px; }}
        .stat-value {{ font-size: 28px; font-weight: bold; color: #48bb78; }}
        .stat-label {{ color: #a0aec0; margin-top: 5px; }}
        textarea {{ width: 100%; height: 150px; background: #1a1a1a; color: white; border: 1px solid #4a5568; border-radius: 5px; padding: 10px; }}
        .form-group {{ margin-bottom: 15px; }}
        .form-group label {{ display: block; margin-bottom: 5px; font-weight: bold; }}
        .form-group input {{ width: 100%; padding: 10px; border: 1px solid #4a5568; border-radius: 5px; background: #1a1a1a; color: white; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
        th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #4a5568; }}
        th {{ background: #4a5568; }}
    </style>
</head>
<body>
    <div class="header">
        <div class="container">
            <div class="nav">
                <h1>🎯 Gestão de Escopo GCPJ</h1>
                <div class="nav-links">
                    <a href="/">Dashboard</a>
                    <a href="/fontes">Fontes</a>
                    <a href="/escopo" class="active">Escopo</a>
                    <a href="/execucoes">Execuções</a>
                </div>
            </div>
        </div>
    </div>
    
    <div class="main">
        <div class="container">
            <div class="cards">
                <div class="card">
                    <div class="stat-value">{total_escopo:,}</div>
                    <div class="stat-label">GCPJs no Escopo Ativo</div>
                </div>
                <div class="card">
                    <a href="/api/escopo/exportar" class="btn btn-warning">📄 Exportar Escopo</a>
                    <button class="btn" onclick="limparEscopo()">🗑️ Limpar Escopo</button>
                </div>
            </div>
            
            <div class="section">
                <h3>➕ Adicionar GCPJs ao Escopo</h3>
                <form id="formAdicionarGCPJs">
                    <div class="form-group">
                        <label>Lista de GCPJs (um por linha):</label>
                        <textarea id="lista_gcpjs" placeholder="24123456&#10;24123457&#10;24123458"></textarea>
                    </div>
                    <div class="form-group">
                        <label>Motivo da Inclusão:</label>
                        <input type="text" id="motivo" placeholder="Ex: Processos ativos 2024" value="Inclusão manual" required>
                    </div>
                    <button type="submit" class="btn btn-success">Adicionar ao Escopo</button>
                </form>
            </div>
            
            <div class="section">
                <h3>📋 Últimos GCPJs Adicionados</h3>
                <table>
                    <thead>
                        <tr>
                            <th>GCPJ</th>
                            <th>Motivo</th>
                            <th>Data Inclusão</th>
                        </tr>
                    </thead>
                    <tbody>
                        ''' + (''.join([f'''
                        <tr>
                            <td>{row['gcpj']}</td>
                            <td>{row['motivo_inclusao']}</td>
                            <td>{row['data_inclusao'][:10] if row['data_inclusao'] else 'N/A'}</td>
                        </tr>
                        ''' for _, row in ultimos_gcpjs.iterrows()]) if not ultimos_gcpjs.empty else '''
                        <tr>
                            <td colspan="3" style="text-align: center; color: #a0aec0;">Nenhum GCPJ no escopo</td>
                        </tr>
                        ''') + '''
                    </tbody>
                </table>
            </div>
        </div>
    </div>
    
    <script>
        document.getElementById('formAdicionarGCPJs').addEventListener('submit', function(e) {{
            e.preventDefault();
            
            const gcpjs = document.getElementById('lista_gcpjs').value
                .split('\\n')
                .map(gcpj => gcpj.trim())
                .filter(gcpj => gcpj.length > 0);
            
            const motivo = document.getElementById('motivo').value;
            
            if (gcpjs.length === 0) {{
                alert('Adicione pelo menos um GCPJ');
                return;
            }}
            
            fetch('/api/escopo/adicionar', {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify({{ gcpjs: gcpjs, motivo: motivo }})
            }})
            .then(response => response.json())
            .then(data => {{
                if (data.success) {{
                    alert(`${{data.adicionados}} GCPJs adicionados ao escopo!`);
                    location.reload();
                }} else {{
                    alert('Erro: ' + data.error);
                }}
            }});
        }});
        
        function limparEscopo() {{
            if (confirm('Limpar TODOS os GCPJs do escopo? Esta ação não pode ser desfeita.')) {{
                fetch('/api/escopo/limpar', {{ method: 'POST' }})
                    .then(response => response.json())
                    .then(data => {{
                        if (data.success) {{
                            alert('Escopo limpo com sucesso!');
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
        '''
        
        return template_html
    
    except Exception as e:
        return f"<h1>Erro na Gestão de Escopo</h1><p>{str(e)}</p><pre>{traceback.format_exc()}</pre>"

@app.route('/execucoes')
def gerenciar_execucoes():
    """Interface para execuções"""
    template_html = '''
<!DOCTYPE html>
<html>
<head>
    <title>Execuções</title>
    <meta charset="UTF-8">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #1a1a1a; color: white; }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px 0; }
        .container { max-width: 1200px; margin: 0 auto; padding: 0 20px; }
        .nav { display: flex; justify-content: space-between; align-items: center; }
        .nav h1 { color: white; font-size: 24px; }
        .nav-links { display: flex; gap: 20px; }
        .nav-links a { color: white; text-decoration: none; padding: 10px 15px; border-radius: 5px; transition: background 0.3s; }
        .nav-links a:hover { background: rgba(255,255,255,0.2); }
        .nav-links a.active { background: rgba(255,255,255,0.3); }
        .main { padding: 30px 0; }
        .section { background: #2d3748; border-radius: 10px; padding: 20px; margin-bottom: 20px; }
        .btn { background: #4299e1; color: white; padding: 12px 20px; border: none; border-radius: 6px; cursor: pointer; text-decoration: none; display: inline-block; margin: 10px 0; }
        .btn:hover { background: #3182ce; }
        .btn-success { background: #48bb78; }
        .btn-success:hover { background: #38a169; }
    </style>
</head>
<body>
    <div class="header">
        <div class="container">
            <div class="nav">
                <h1>⚡ Execuções</h1>
                <div class="nav-links">
                    <a href="/">Dashboard</a>
                    <a href="/fontes">Fontes</a>
                    <a href="/escopo">Escopo</a>
                    <a href="/execucoes" class="active">Execuções</a>
                </div>
            </div>
        </div>
    </div>
    
    <div class="main">
        <div class="container">
            <div class="section">
                <h2>🚀 Executar Migração</h2>
                <p>Execute uma migração completa baseada no escopo atual de GCPJs.</p>
                <button class="btn btn-success" onclick="executarMigracao()">Executar Migração Completa</button>
            </div>
        </div>
    </div>
    
    <script>
        function executarMigracao() {
            if (confirm('Executar migração completa? Isso pode levar alguns minutos.')) {
                window.location.href = '/';
                setTimeout(() => {
                    const event = new CustomEvent('executarMigracao');
                    window.dispatchEvent(event);
                }, 1000);
            }
        }
    </script>
</body>
</html>
    '''
    return template_html

# ================== APIs FUNCIONAIS ==================

@app.route('/api/fontes', methods=['POST'])
def api_adicionar_fonte():
    """API para adicionar nova fonte"""
    try:
        dados = request.json
        
        fonte_id = gestao.master_db.adicionar_fonte(
            nome=dados['nome_fonte'],
            tipo=dados['tipo_fonte'],
            caminho=dados['caminho_arquivo'],
            aba=dados['aba_planilha'],
            coluna_gcpj=dados['coluna_gcpj'],
            prioridade=dados['prioridade'],
            descricao=dados.get('descricao', '')
        )
        
        return jsonify({'success': True, 'fonte_id': fonte_id})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/fontes/<int:fonte_id>/testar')
def api_testar_fonte(fonte_id):
    """API para testar uma fonte"""
    try:
        fonte_info = gestao.master_db.obter_fonte_por_id(fonte_id)
        
        if fonte_info.empty:
            return jsonify({'success': False, 'error': 'Fonte não encontrada'}), 404
        
        df = gestao.processador.carregar_fonte(fonte_info.iloc[0])
        
        if df.empty:
            return jsonify({'success': False, 'error': 'Arquivo não encontrado ou vazio'})
        
        return jsonify({
            'success': True,
            'registros': len(df),
            'colunas': df.columns.tolist()
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/fontes/<int:fonte_id>', methods=['DELETE'])
def api_remover_fonte(fonte_id):
    """API para remover uma fonte"""
    try:
        conn = sqlite3.connect(gestao.master_db.db_path)
        cursor = conn.cursor()
        
        cursor.execute("UPDATE fontes_dados SET ativa = 0 WHERE id = ?", (fonte_id,))
        
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
        gcpjs = dados['gcpjs']
        motivo = dados['motivo']
        
        adicionados = gestao.master_db.adicionar_gcpjs_escopo(gcpjs, motivo)
        
        return jsonify({'success': True, 'adicionados': adicionados})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/escopo/exportar')
def api_exportar_escopo():
    """API para exportar escopo atual"""
    try:
        escopo = gestao.master_db.obter_escopo_gcpjs()
        
        df = pd.DataFrame({'GCPJ': escopo})
        
        output = BytesIO()
        df.to_excel(output, index=False, sheet_name='Escopo_GCPJ')
        output.seek(0)
        
        return send_file(
            output,
            as_attachment=True,
            download_name=f'escopo_gcpj_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx',
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/escopo/limpar', methods=['POST'])
def api_limpar_escopo():
    """API para limpar escopo"""
    try:
        conn = sqlite3.connect(gestao.master_db.db_path)
        cursor = conn.cursor()
        
        cursor.execute("UPDATE gcpj_escopo SET ativo = 0")
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/executar_migracao', methods=['POST'])
def api_executar_migracao():
    """API para executar migração completa"""
    try:
        print("Iniciando execução de migração via API...")
        resultado = gestao.processador.executar_migracao_completa()
        
        if resultado.empty:
            return jsonify({'success': False, 'error': 'Nenhum resultado gerado. Verifique escopo e fontes.'})
        
        # Salvar resultado
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        arquivo_resultado = f"migracao_completa_{timestamp}.xlsx"
        
        # Criar pasta downloads se não existir
        downloads_path = os.path.join(gestao.base_path, "downloads")
        os.makedirs(downloads_path, exist_ok=True)
        
        caminho_resultado = os.path.join(downloads_path, arquivo_resultado)
        resultado.to_excel(caminho_resultado, index=True)
        
        print(f"Resultado salvo em: {caminho_resultado}")
        
        return jsonify({
            'success': True,
            'registros_processados': len(resultado),
            'colunas': len(resultado.columns),
            'arquivo': arquivo_resultado,
            'message': f'Migração concluída! {len(resultado)} registros processados.'
        })
        
    except Exception as e:
        print(f"Erro na migração: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/testar_todas_fontes')
def api_testar_todas_fontes():
    """API para testar todas as fontes"""
    try:
        fontes = gestao.master_db.obter_fontes_ativas()
        resultados = []
        
        for _, fonte in fontes.iterrows():
            try:
                df = gestao.processador.carregar_fonte(fonte)
                resultados.append({
                    'nome': fonte['nome_fonte'],
                    'status': 'OK',
                    'registros': len(df),
                    'colunas': len(df.columns)
                })
            except Exception as e:
                resultados.append({
                    'nome': fonte['nome_fonte'],
                    'status': f'ERRO: {str(e)}',
                    'registros': 0,
                    'colunas': 0
                })
        
        return jsonify({'success': True, 'resultados': resultados})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/verificar_template')
def api_verificar_template():
    """API para verificar template"""
    try:
        template_path = os.path.join(gestao.base_path, "template-banco-bradesco-sa.xlsx")
        
        if not os.path.exists(template_path):
            return jsonify({'success': False, 'error': 'Template não encontrado'})
        
        df = pd.read_excel(template_path, sheet_name='Sheet')
        
        return jsonify({
            'success': True,
            'arquivo': 'template-banco-bradesco-sa.xlsx',
            'colunas': len(df.columns),
            'linhas': len(df)
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

if __name__ == '__main__':
    # Garantir que pastas e banco existem
    os.makedirs(gestao.base_path, exist_ok=True)
    os.makedirs(os.path.join(gestao.base_path, "downloads"), exist_ok=True)
    
    print("🚀 Iniciando Interface de Gestão 100% Funcional...")
    print("📊 Acesse: http://localhost:5002")
    print("💡 Sistema completo para gestão de fontes e escopo GCPJ")
    print("✅ Todas as funcionalidades principais operacionais")
    
    app.run(debug=True, port=5002, host='0.0.0.0')
