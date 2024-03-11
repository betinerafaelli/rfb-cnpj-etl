#
# L2: Carrega os arquivos .csv para um banco DuckDB. (5.5 GB, ~1.5 min)
#

INPUT_FOLDER = ".data/L1-csv"
OUTPUT_FOLDER = ".data/L2-silver"


#
# Functions
#

from glob import glob
from os import makedirs
from os.path import abspath, join

import duckdb


def has_table(con: duckdb.DuckDBPyConnection, table_name: str):
    r = con.sql("SELECT 1 FROM duckdb_tables WHERE table_name = ?", params=[table_name])
    return r.fetchone() is not None


def csv_to_duckdb(con: duckdb.DuckDBPyConnection, table_name: str, glob_pattern: str, schema, select=None):
    if not has_table(con, table_name):
        input_files = glob(join(INPUT_FOLDER, glob_pattern))
        csv = con.read_csv(
            input_files,
            header=False,
            delimiter=";",
            names=list(schema.keys()),
            dtype=list(schema.values()),
            encoding="utf-8",
            parallel=True,
            date_format="%Y%m%d",
        )
        select = "SELECT * FROM csv" if select is None else select
        con.sql(f"CREATE TABLE {table_name} AS {select}")

    print(f"    {table_name}")


def regime_tributacao_csv_to_duckdb(con: duckdb.DuckDBPyConnection, table_name: str, glob_pattern: str):
    if not has_table(con, table_name):
        ddl = rf"""
            CREATE TABLE {table_name} (
                ano USMALLINT,
                cnpj VARCHAR,
                cnpj_scp VARCHAR,
                tributacao VARCHAR,
                qtd UTINYINT
            );
        """
        con.sql(ddl)

        input_files = glob(abspath(join(INPUT_FOLDER, glob_pattern)))
        for csv_file in input_files:
            # Lê primeira linha do arquivo
            with open(csv_file, "r", encoding="utf-8") as f:
                first_line = f.readline()

            # Alguns arquivos do regime de tributacao possuem cabeçalho e usam ',' como separador.
            if first_line.startswith("ano,cnpj"):
                con.sql(f"COPY {table_name} FROM '{csv_file}' (FORMAT CSV, DELIMITER ',', HEADER)")
            else:
                con.sql(f"COPY {table_name} FROM '{csv_file}' (FORMAT CSV, DELIMITER ';')")

    print(f"    {table_name}")


#
# Main
#


def main():
    UInt32 = "UINTEGER"
    UInt16 = "USMALLINT"
    UInt8 = "UTINYINT"
    Utf8 = "VARCHAR"

    schema_cnae = {"codigo": UInt32, "descricao": Utf8}

    schema_satelites = {"codigo": UInt16, "descricao": Utf8}

    schema_empresa = {
        "cnpj_base": UInt32,
        "razao_social": Utf8,
        "natureza_juridica": UInt16,
        "qualificacao_responsavel": UInt8,
        "capital_social_str": Utf8,
        "porte_empresa": UInt8,
        "ente_federativo_responsavel": Utf8,
    }

    schema_estabelecimento = {
        "cnpj_base": UInt32,
        "cnpj_ordem": UInt16,
        "cnpj_dv": UInt8,
        "matriz": UInt8,
        "nome_fantasia": Utf8,
        "situacao_cadastral": UInt8,
        "data_situacao_cadastral": Utf8,
        "motivo_situacao_cadastral": UInt8,
        "nome_cidade_exterior": Utf8,
        "pais": Utf8,
        "data_inicio_atividades": Utf8,
        "cnae": Utf8,
        "cnae_secundario": Utf8,
        "tipo_logradouro": Utf8,
        "logradouro": Utf8,
        "numero": Utf8,
        "complemento": Utf8,
        "bairro": Utf8,
        "cep": Utf8,
        "uf": Utf8,
        "municipio": UInt16,
        "ddd1": Utf8,
        "telefone1": Utf8,
        "ddd2": Utf8,
        "telefone2": Utf8,
        "ddd_fax": Utf8,
        "fax": Utf8,
        "correio_eletronico": Utf8,
        "situacao_especial": Utf8,
        "data_situacao_especial": Utf8,
    }

    schema_simples = {
        "cnpj_base": UInt32,
        "opcao_simples": Utf8,
        "data_opcao_simples": Utf8,
        "data_exclusao_simples": Utf8,
        "opcao_mei": Utf8,
        "data_opcao_mei": Utf8,
        "data_exclusao_mei": Utf8,
    }

    makedirs(OUTPUT_FOLDER, exist_ok=True)
    print("L2: Creating database...")

    db_file = join(OUTPUT_FOLDER, "rfb-cnpj.duckdb")
    con = duckdb.connect(db_file)
    con.sql("SET preserve_insertion_order = false")

    csv_to_duckdb(con, "cnae", "*.CNAECSV", schema_cnae)
    csv_to_duckdb(con, "motivo", "*.MOTICSV", schema_satelites)
    csv_to_duckdb(con, "municipio", "*.MUNICCSV", schema_satelites)
    csv_to_duckdb(con, "natureza_juridica", "*.NATJUCSV", schema_satelites)
    csv_to_duckdb(con, "pais", "*.PAISCSV", schema_satelites)

    csv_to_duckdb(con, "empresa", "*.EMPRECSV", schema_empresa)
    csv_to_duckdb(
        con,
        "estabelecimento",
        "*.ESTABELE",
        schema_estabelecimento,
    )
    csv_to_duckdb(con, "simples", "*.SIMPLES.CSV.*", schema_simples)

    # Tratamento especial para arquivos do regime de tributação.
    regime_tributacao_csv_to_duckdb(con, "regime_tributacao", "*.csv")

    print(f"L2: Database ready.")


if __name__ == "__main__":
    main()
