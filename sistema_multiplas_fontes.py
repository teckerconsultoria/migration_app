"""
Sistema Avançado de Migração com Múltiplas Fontes e Escopo GCPJ

FUNCIONALIDADES:
1. Múltiplas fontes primárias configuráveis por coluna
2. Sistema de priorização e fallback entre fontes  
3. Escopo de GCPJs controlado via SQLite
4. Master Database como repositório central
5. Interface para configuração dinâmica

ARQUITETURA:
master_database.db
├── gcpj_escopo (GCPJs válidos para exportação)
├── fontes_dados (configuração das fontes disponíveis)
├── mapeamento_colunas (qual fonte usar para cada coluna)
├── template_colunas (estrutura do template)
└── execucoes_historico (log de execuções)
"""

import sqlite3
import pandas as pd
import os
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

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
        
        # 1. Tabela de escopo de GCPJs (controla quais GCPJs exportar)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gcpj_escopo (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                gcpj TEXT UNIQUE NOT NULL,
                ativo BOOLEAN DEFAULT 1,
                motivo_inclusao TEXT,
                data_inclusao TEXT,
                criterios_filtro TEXT,
                observacoes TEXT
            )
        ''')
        
        # 2. Tabela de fontes de dados disponíveis
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fontes_dados (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome_fonte TEXT UNIQUE NOT NULL,
                tipo_fonte TEXT NOT NULL, -- 'excel', 'csv', 'database'
                caminho_arquivo TEXT,
                aba_planilha TEXT,
                coluna_gcpj TEXT NOT NULL,
                ativa BOOLEAN DEFAULT 1,
                prioridade INTEGER DEFAULT 10, -- menor = maior prioridade
                descricao TEXT,
                data_criacao TEXT
            )
        ''')
        
        # 3. Tabela de mapeamento por coluna (qual fonte usar para cada coluna)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS mapeamento_colunas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                coluna_template TEXT NOT NULL,
                fonte_id INTEGER,
                coluna_origem TEXT NOT NULL,
                prioridade INTEGER DEFAULT 10,
                ativa BOOLEAN DEFAULT 1,
                tipo_mapeamento TEXT DEFAULT 'direto', -- 'direto', 'constante', 'calculado'
                valor_constante TEXT,
                formula_calculo TEXT,
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
                categoria TEXT -- 'identificacao', 'financeiro', 'juridico', etc.
            )
        ''')
        
        # 5. Tabela de critérios de escopo (regras para inclusão automática)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS criterios_escopo (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome_criterio TEXT NOT NULL,
                fonte_verificacao TEXT NOT NULL,
                condicao_sql TEXT NOT NULL,
                ativo BOOLEAN DEFAULT 1,
                descricao TEXT,
                data_criacao TEXT
            )
        ''')
        
        # 6. Tabela de histórico de execuções detalhadas
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS execucoes_master (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                tipo_execucao TEXT NOT NULL, -- 'diagnostico', 'migracao', 'validacao'
                configuracao_fontes TEXT, -- JSON com config usada
                escopo_gcpjs INTEGER, -- quantidade de GCPJs no escopo
                registros_processados INTEGER,
                registros_exportados INTEGER,
                taxa_sucesso REAL,
                observacoes TEXT,
                arquivo_resultado TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
        
        # Inicializar dados básicos se não existirem
        self.inicializar_dados_basicos()
    
    def inicializar_dados_basicos(self):
        """Inicializa dados básicos se o banco estiver vazio"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Verificar se já existem dados
        cursor.execute("SELECT COUNT(*) FROM fontes_dados")
        if cursor.fetchone()[0] == 0:
            logger.info("Inicializando dados básicos do master database...")
            
            # Fontes padrão
            fontes_padrao = [
                ("Fonte Principal GCPJ", "excel", "cópia-MOYA E LARA_BASE GCPJ ATIVOS - 07_04_2025.xlsx", "Sheet1", "GCPJ", 1, 1, "Fonte principal com dados jurídicos"),
                ("Base Ativa Escritório", "excel", "4.MOYA E LARA SOCIEDADE DE ADVOGADOS_PRÉVIA BASE ATIVA _ABRIL_disp_24_04_2025.xlsx", "Sheet1", "GCPJ", 1, 2, "Base secundária via GCPJ"),
                ("Contratos Específicos", "excel", "", "Sheet1", "GCPJ", 0, 3, "Fonte futura para dados de contratos"),
                ("Dados Financeiros", "excel", "", "Sheet1", "GCPJ", 0, 4, "Fonte futura para informações financeiras")
            ]
            
            cursor.executemany('''
                INSERT INTO fontes_dados (nome_fonte, tipo_fonte, caminho_arquivo, aba_planilha, coluna_gcpj, ativa, prioridade, descricao)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', fontes_padrao)
            
            # Template colunas básicas
            colunas_template = [
                ("CÓD. INTERNO", 1, "texto", 1, "Código interno GCPJ", "identificacao"),
                ("PROCESSO", 2, "texto", 1, "Número do processo", "juridico"),
                ("PROCEDIMENTO", 3, "texto", 0, "Tipo de ação/procedimento", "juridico"),
                ("CPF/CNPJ", 4, "texto", 1, "Documento da parte", "identificacao"),
                ("SEGMENTO DO CONTRATO", 5, "texto", 0, "Segmento do contrato", "financeiro"),
                ("OPERAÇÃO", 6, "texto", 0, "Tipo de operação", "financeiro")
            ]
            
            cursor.executemany('''
                INSERT INTO template_colunas (coluna_nome, posicao, tipo_dados, obrigatoria, descricao, categoria)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', colunas_template)
        
        conn.commit()
        conn.close()
    
    # ======= GESTÃO DE ESCOPO DE GCPJs =======
    
    def adicionar_gcpjs_escopo(self, gcpjs: List[str], motivo: str = "Inclusão manual"):
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
                logger.warning(f"Erro ao adicionar GCPJ {gcpj}: {str(e)}")
        
        conn.commit()
        conn.close()
        
        logger.info(f"Adicionados {sucesso} GCPJs ao escopo (de {len(gcpjs)} tentativas)")
        return sucesso
    
    def obter_escopo_gcpjs(self, apenas_ativos=True) -> List[str]:
        """Obtém lista de GCPJs no escopo"""
        conn = sqlite3.connect(self.db_path)
        
        where_clause = "WHERE ativo = 1" if apenas_ativos else ""
        df = pd.read_sql_query(f"SELECT gcpj FROM gcpj_escopo {where_clause} ORDER BY gcpj", conn)
        
        conn.close()
        return df['gcpj'].tolist()
    
    def aplicar_criterio_escopo(self, nome_criterio: str, fonte_dados: str, condicao_sql: str):
        """Aplica critério para inclusão automática de GCPJs"""
        # Por exemplo: incluir todos os GCPJs da fonte principal que tenham PROCESSO preenchido
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Salvar critério
        cursor.execute('''
            INSERT OR REPLACE INTO criterios_escopo (nome_criterio, fonte_verificacao, condicao_sql, data_criacao)
            VALUES (?, ?, ?, ?)
        ''', (nome_criterio, fonte_dados, condicao_sql, datetime.now().isoformat()))
        
        # TODO: Implementar execução do critério SQL na fonte
        
        conn.commit()
        conn.close()
    
    # ======= GESTÃO DE FONTES =======
    
    def adicionar_fonte(self, nome: str, tipo: str, caminho: str, aba: str, coluna_gcpj: str, prioridade: int = 10):
        """Adiciona nova fonte de dados"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO fontes_dados (nome_fonte, tipo_fonte, caminho_arquivo, aba_planilha, coluna_gcpj, prioridade, data_criacao)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (nome, tipo, caminho, aba, coluna_gcpj, prioridade, datetime.now().isoformat()))
        
        fonte_id = cursor.lastrowid
        
        conn.commit()
        conn.close()
        
        logger.info(f"Fonte '{nome}' adicionada com ID {fonte_id}")
        return fonte_id
    
    def obter_fontes_ativas(self) -> pd.DataFrame:
        """Obtém todas as fontes ativas ordenadas por prioridade"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query('''
            SELECT * FROM fontes_dados 
            WHERE ativa = 1 
            ORDER BY prioridade ASC
        ''', conn)
        conn.close()
        return df
    
    # ======= GESTÃO DE MAPEAMENTOS =======
    
    def configurar_mapeamento_coluna(self, coluna_template: str, fonte_id: int, coluna_origem: str, prioridade: int = 10):
        """Configura de onde vem cada coluna do template"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO mapeamento_colunas 
            (coluna_template, fonte_id, coluna_origem, prioridade)
            VALUES (?, ?, ?, ?)
        ''', (coluna_template, fonte_id, coluna_origem, prioridade))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Mapeamento configurado: {coluna_template} ← {coluna_origem} (fonte {fonte_id})")
    
    def obter_mapeamentos_coluna(self, coluna_template: str) -> pd.DataFrame:
        """Obtém mapeamentos para uma coluna específica, ordenados por prioridade"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query('''
            SELECT m.*, f.nome_fonte, f.caminho_arquivo, f.coluna_gcpj
            FROM mapeamento_colunas m
            JOIN fontes_dados f ON m.fonte_id = f.id
            WHERE m.coluna_template = ? AND m.ativa = 1 AND f.ativa = 1
            ORDER BY m.prioridade ASC
        ''', conn, params=(coluna_template,))
        conn.close()
        return df
    
    def obter_todos_mapeamentos(self) -> pd.DataFrame:
        """Obtém todos os mapeamentos ativos"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query('''
            SELECT m.coluna_template, m.coluna_origem, m.prioridade, 
                   f.nome_fonte, f.tipo_fonte, f.caminho_arquivo
            FROM mapeamento_colunas m
            JOIN fontes_dados f ON m.fonte_id = f.id
            WHERE m.ativa = 1 AND f.ativa = 1
            ORDER BY m.coluna_template, m.prioridade ASC
        ''', conn)
        conn.close()
        return df


class ProcessadorMultiplasFontes:
    """Processador que utiliza múltiplas fontes baseado no master database"""
    
    def __init__(self, base_path="C:/desenvolvimento/migration_app"):
        self.base_path = base_path
        self.master_db = MasterDatabaseManager(base_path)
        self.fontes_carregadas = {}
    
    def carregar_fonte(self, fonte_info: pd.Series) -> pd.DataFrame:
        """Carrega uma fonte específica"""
        fonte_id = fonte_info['id']
        
        # Cache de fontes
        if fonte_id in self.fontes_carregadas:
            return self.fontes_carregadas[fonte_id]
        
        try:
            caminho_completo = os.path.join(self.base_path, fonte_info['caminho_arquivo'])
            
            if fonte_info['tipo_fonte'] == 'excel':
                df = pd.read_excel(caminho_completo, sheet_name=fonte_info['aba_planilha'])
            elif fonte_info['tipo_fonte'] == 'csv':
                df = pd.read_csv(caminho_completo)
            else:
                raise ValueError(f"Tipo de fonte não suportado: {fonte_info['tipo_fonte']}")
            
            # Normalizar coluna GCPJ
            if fonte_info['coluna_gcpj'] in df.columns:
                df[fonte_info['coluna_gcpj']] = df[fonte_info['coluna_gcpj']].astype(str).str.strip()
            
            self.fontes_carregadas[fonte_id] = df
            logger.info(f"Fonte '{fonte_info['nome_fonte']}' carregada: {len(df)} registros")
            
            return df
            
        except Exception as e:
            logger.error(f"Erro ao carregar fonte {fonte_info['nome_fonte']}: {str(e)}")
            return pd.DataFrame()
    
    def processar_coluna_multiplas_fontes(self, coluna_template: str, escopo_gcpjs: List[str]) -> pd.Series:
        """Processa uma coluna usando múltiplas fontes com fallback"""
        mapeamentos = self.master_db.obter_mapeamentos_coluna(coluna_template)
        
        if mapeamentos.empty:
            logger.warning(f"Nenhum mapeamento encontrado para coluna {coluna_template}")
            return pd.Series(index=escopo_gcpjs, dtype=object)
        
        # Resultado final
        resultado = pd.Series(index=escopo_gcpjs, dtype=object)
        
        # Processar fontes por ordem de prioridade
        for _, mapeamento in mapeamentos.iterrows():
            fonte_info = mapeamento
            df_fonte = self.carregar_fonte(fonte_info)
            
            if df_fonte.empty:
                continue
            
            # Criar mapeamento GCPJ → valor
            if fonte_info['coluna_gcpj'] in df_fonte.columns and mapeamento['coluna_origem'] in df_fonte.columns:
                mapa_valores = df_fonte.set_index(fonte_info['coluna_gcpj'])[mapeamento['coluna_origem']].to_dict()
                
                # Preencher valores faltantes
                for gcpj in escopo_gcpjs:
                    if pd.isna(resultado[gcpj]) and gcpj in mapa_valores and pd.notna(mapa_valores[gcpj]):
                        resultado[gcpj] = mapa_valores[gcpj]
        
        preenchidos = resultado.notna().sum()
        logger.info(f"Coluna {coluna_template}: {preenchidos}/{len(escopo_gcpjs)} preenchidos ({preenchidos/len(escopo_gcpjs)*100:.1f}%)")
        
        return resultado
    
    def executar_migracao_completa(self, incluir_todos_template=True) -> pd.DataFrame:
        """Executa migração completa usando configuração do master database"""
        # Obter escopo de GCPJs
        escopo_gcpjs = self.master_db.obter_escopo_gcpjs()
        
        if not escopo_gcpjs:
            logger.error("Nenhum GCPJ no escopo. Adicione GCPJs primeiro.")
            return pd.DataFrame()
        
        logger.info(f"Processando {len(escopo_gcpjs)} GCPJs no escopo")
        
        # Obter colunas do template
        conn = sqlite3.connect(self.master_db.db_path)
        colunas_template = pd.read_sql_query('''
            SELECT coluna_nome FROM template_colunas ORDER BY posicao
        ''', conn)['coluna_nome'].tolist()
        conn.close()
        
        # Resultado final
        resultado_df = pd.DataFrame(index=escopo_gcpjs, columns=colunas_template)
        
        # Processar cada coluna
        for coluna in colunas_template:
            try:
                resultado_df[coluna] = self.processar_coluna_multiplas_fontes(coluna, escopo_gcpjs)
            except Exception as e:
                logger.error(f"Erro ao processar coluna {coluna}: {str(e)}")
                resultado_df[coluna] = None
        
        # Salvar execução no histórico
        self.salvar_execucao_historico("migracao", len(escopo_gcpjs), len(resultado_df))
        
        return resultado_df
    
    def salvar_execucao_historico(self, tipo: str, escopo: int, processados: int):
        """Salva execução no histórico do master database"""
        conn = sqlite3.connect(self.master_db.db_path)
        cursor = conn.cursor()
        
        configuracao = {
            'fontes_ativas': len(self.master_db.obter_fontes_ativas()),
            'mapeamentos_ativos': len(self.master_db.obter_todos_mapeamentos())
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
            processados,  # assumindo que todos foram exportados
            100.0 if processados > 0 else 0.0
        ))
        
        conn.commit()
        conn.close()


# ======= EXEMPLO DE USO =======

def exemplo_configuracao_sistema():
    """Exemplo de como configurar o sistema"""
    
    # 1. Inicializar master database
    master = MasterDatabaseManager()
    
    # 2. Adicionar fonte específica de contratos
    fonte_contratos_id = master.adicionar_fonte(
        nome="Contratos Segmentos",
        tipo="excel", 
        caminho="dados_contratos_segmentos.xlsx",
        aba="Contratos",
        coluna_gcpj="CODIGO_GCPJ",
        prioridade=1  # Alta prioridade para dados específicos
    )
    
    # 3. Configurar mapeamentos específicos
    master.configurar_mapeamento_coluna("SEGMENTO DO CONTRATO", fonte_contratos_id, "SEGMENTO_DETALHADO", prioridade=1)
    master.configurar_mapeamento_coluna("TIPO DE OPERAÇÃO/CARTEIRA", fonte_contratos_id, "CARTEIRA_ESPECIFICA", prioridade=1)
    
    # 4. Adicionar GCPJs ao escopo baseado em critério
    gcpjs_exemplo = ["24123456", "24123457", "24123458"]
    master.adicionar_gcpjs_escopo(gcpjs_exemplo, "GCPJs com processos ativos")
    
    # 5. Executar migração
    processador = ProcessadorMultiplasFontes()
    resultado = processador.executar_migracao_completa()
    
    print(f"Migração concluída: {len(resultado)} registros processados")
    return resultado


if __name__ == "__main__":
    # Executar exemplo
    resultado = exemplo_configuracao_sistema()
