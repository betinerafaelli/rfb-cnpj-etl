#
# L3: Consolida e refina informações em um novo banco DuckDB. (5.8 GB, ~4 min)
#

INPUT_FOLDER = ".data/L2-silver"
OUTPUT_FOLDER = ".data/L3-gold"


#
# Functions
#

from os import makedirs
from os.path import abspath, join

import duckdb


#
# Main
#
def create_table_from_sql(con: duckdb.DuckDBPyConnection, table_name: str, sql: str = None):
    sql = f"SELECT * from input.{table_name}" if sql is None else sql
    con.sql(f"CREATE TABLE IF NOT EXISTS {table_name} AS {sql}")
    print(f"    {table_name}")


def main():
    makedirs(OUTPUT_FOLDER, exist_ok=True)
    print("L3: Creating database...")

    db_file = join(OUTPUT_FOLDER, "rfb-cnpj.duckdb")
    con = duckdb.connect(db_file)
    con.sql("SET preserve_insertion_order = false")

    input_db_file = abspath(join(INPUT_FOLDER, "rfb-cnpj.duckdb"))
    con.sql(f"ATTACH '{input_db_file}' AS input")

    # Empresa
    sql_empresa = r"""
        SELECT
            empresa.cnpj_base,
            empresa.razao_social,
            empresa.natureza_juridica,
            empresa.qualificacao_responsavel,
            NULLIF(TRY_CAST(replace(empresa.capital_social_str, ',', '.') AS NUMERIC), 0) AS capital_social,
            empresa.porte_empresa,
            empresa.ente_federativo_responsavel,
            simples.opcao_simples = 'S' AS opcao_simples,
            CAST(try_strptime(simples.data_opcao_simples, '%Y%m%d') AS DATE) AS data_opcao_simples,
            CAST(try_strptime(simples.data_exclusao_simples, '%Y%m%d') AS DATE) AS data_exclusao_simples,
            simples.opcao_mei = 'S' AS opcao_mei,
            CAST(try_strptime(simples.data_opcao_mei, '%Y%m%d') AS DATE) AS data_opcao_mei,
            CAST(try_strptime(simples.data_exclusao_mei, '%Y%m%d') AS DATE) AS data_exclusao_mei
        FROM
            input.empresa
            LEFT JOIN input.simples
              ON simples.cnpj_base = empresa.cnpj_base
    """
    create_table_from_sql(con, "empresa", sql_empresa)

    # Estabelecimento
    sql_estabelecimento = r"""
        WITH rt AS (
            -- Retorna o regime de tributacao do maior ano para cada cnpj.
            SELECT
                cnpj, arg_max(tributacao, ano) AS regime_tributacao
            FROM 
                input.regime_tributacao
            GROUP BY 
                cnpj
        )
        SELECT 
            estabelecimento.cnpj_base,
            estabelecimento.cnpj_ordem,
            estabelecimento.cnpj_dv,
            estabelecimento.matriz == 1 AS matriz,
            estabelecimento.nome_fantasia,
            estabelecimento.situacao_cadastral,
            CAST(try_strptime(estabelecimento.data_situacao_cadastral, '%Y%m%d') AS DATE) AS data_situacao_cadastral,
            estabelecimento.motivo_situacao_cadastral,
            estabelecimento.nome_cidade_exterior,
            estabelecimento.pais,
            CAST(try_strptime(estabelecimento.data_inicio_atividades, '%Y%m%d') AS DATE) AS data_inicio_atividades,
            data_inicio_atividades,
            estabelecimento.cnae,
            estabelecimento.cnae_secundario,
            estabelecimento.tipo_logradouro,
            estabelecimento.logradouro,
            CASE
                WHEN estabelecimento.numero IN ('S/N', 'SN', 'S N', 'sn', 's/n', 'S/Nº', 'S/N.', '-', 'S/NR', '.', 'Sn', 'SNR', 'S.N', 'S-N', 'SEM', 'SEM NM', 'SEM NU', 'S\N', 'S/n', '0', '00', '000', '0000', '00000', '000000')
                THEN NULL
                ELSE estabelecimento.numero
            END AS numero,
            estabelecimento.complemento,
            estabelecimento.bairro,
            LPAD(NULLIF(estabelecimento.cep, '0'), 8, '0') AS cep,
            estabelecimento.uf,
            estabelecimento.municipio,
            estabelecimento.ddd1,
            estabelecimento.telefone1,
            estabelecimento.ddd2,
            estabelecimento.telefone2,
            estabelecimento.ddd_fax,
            estabelecimento.fax,
            estabelecimento.correio_eletronico,
            estabelecimento.situacao_especial,
            CAST(try_strptime(estabelecimento.data_situacao_especial, '%Y%m%d') AS DATE) AS data_situacao_especial,
            rt.regime_tributacao
        FROM
            input.estabelecimento
            LEFT JOIN rt
                   ON rt.cnpj = format('{:t.}/{:04d}-{:02d}', 100000000 + CAST(cnpj_base AS INTEGER), CAST(cnpj_ordem AS INTEGER), CAST(cnpj_dv AS INTEGER))[2:19]
    """
    create_table_from_sql(con, "estabelecimento", sql_estabelecimento)

    create_table_from_sql(con, "cnae")
    create_table_from_sql(con, "motivo")
    create_table_from_sql(con, "municipio")
    create_table_from_sql(con, "natureza_juridica")
    create_table_from_sql(con, "pais")

    con.sql(f"DETACH input")

    print(f"L3: Database ready.")


if __name__ == "__main__":
    main()
