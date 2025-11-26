import json
import os
import glob
import mysql.connector
from mysql.connector import Error

# ==================== CONFIGURAÇÃO DO BANCO ====================
DB_CONFIG = {
    'host': 'localhost',
    'database': 'resilead',
    'user': 'sinir',
    'password': 'sinir123*456',
    'charset': 'utf8mb4',
    'use_unicode': True,
    'autocommit': True
}

# Diretório onde estão os 27 arquivos JSON (um por estado)
JSON_DIR = './'   # ajuste o caminho se necessário

# ==============================================================
def carregar_municipios():
    conexao = None
    try:
        conexao = mysql.connector.connect(**DB_CONFIG)
        cursor = conexao.cursor()

        print("Limpando tabela municipio (opcional)...")
        cursor.execute("TRUNCATE TABLE municipio")

        total_municipios = 0
        arquivos = glob.glob(os.path.join(JSON_DIR, "*.json"))
        print(f"Encontrados {len(arquivos)} arquivos JSON.\n")

        for arquivo in arquivos:
            nome_arquivo = os.path.basename(arquivo)
            print(f"Processando {nome_arquivo}...")

            with open(arquivo, 'r', encoding='utf-8') as f:
                municipios = json.load(f)

            inseridos = 0
            for mun in municipios:
                # --- Campos obrigatórios ---
                codigo_municipio = mun['id']
                nome_municipio = mun['nome']

                # --- Sigla do estado (sempre presente em pelo menos uma das duas hierarquias) ---
                # Prioriza a região intermediária (nunca é null), fallback para microrregiao se existir
                if mun['regiao-imediata'] and mun['regiao-imediata']['regiao-intermediaria'] and mun['regiao-imediata']['regiao-intermediaria']['UF']:
                    sigla_estado = mun['regiao-imediata']['regiao-intermediaria']['UF']['sigla']
                elif mun.get('microrregiao') and mun['microrregiao'].get('mesorregiao') and mun['microrregiao']['mesorregiao'].get('UF'):
                    sigla_estado = mun['microrregiao']['mesorregiao']['UF']['sigla']
                else:
                    raise ValueError(f"Não foi possível determinar a UF do município {codigo_municipio}")

                # --- Microrregião e Mesorregião (podem ser null) ---
                micro_id = micro_nome = meso_id = meso_nome = None

                if mun.get('microrregiao') and mun['microrregiao'] is not None:
                    micro = mun['microrregiao']
                    micro_id = micro['id']
                    micro_nome = micro['nome']

                    if micro.get('mesorregiao') and micro['mesorregiao'] is not None:
                        meso = micro['mesorregiao']
                        meso_id = meso['id']
                        meso_nome = meso['nome']

                # --- Região Imediata e Intermediária (sempre presentes) ---
                reg_imediata = mun['regiao-imediata']
                reg_intermed = reg_imediata['regiao-intermediaria']

                sql = """
                    INSERT INTO municipio (
                        codigo_municipio_ibge, municipio, codigo_estado,
                        codigo_microregiao_ibge, microregiao,
                        codigo_mesoregiao_ibge, mesoregiao,
                        codigo_regiao_imediata_ibge, regiao_imediata,
                        codigo_regiao_intermediaria_ibge, regiao_intermediaria
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON DUPLICATE KEY UPDATE
                        municipio = VALUES(municipio),
                        codigo_estado = VALUES(codigo_estado),
                        codigo_microregiao_ibge = VALUES(codigo_microregiao_ibge),
                        microregiao = VALUES(microregiao),
                        codigo_mesoregiao_ibge = VALUES(codigo_mesoregiao_ibge),
                        mesoregiao = VALUES(mesoregiao),
                        regiao_imediata = VALUES(regiao_imediata),
                        regiao_intermediaria = VALUES(regiao_intermediaria);
                """

                valores = (
                    codigo_municipio,
                    nome_municipio,
                    sigla_estado,
                    micro_id,           # pode ser None → vira NULL no MySQL
                    micro_nome,         # pode ser None
                    meso_id,            # pode ser None
                    meso_nome,          # pode ser None
                    reg_imediata['id'],
                    reg_imediata['nome'],
                    reg_intermed['id'],
                    reg_intermed['nome']
                )

                cursor.execute(sql, valores)
                inseridos += 1
                total_municipios += 1

            print(f"  → {inseridos} municípios processados em {nome_arquivo}")

        print("   (5570 até 2021 | 5571 a partir de 2022 com Fernando de Noronha)")
        print(f"   → {total_municipios} é o número correto para 2025")        

    except Error as e:
        print(f"Erro no MySQL: {e}")
    except Exception as e:
        print(f"Erro geral: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conexao and conexao.is_connected():
            cursor.close()
            conexao.close()
            print("Conexão fechada.")

if __name__ == "__main__":
    carregar_municipios()