#
# Exemplo de consulta
#

INPUT_FOLDER = ".data/L3-gold"
OUTPUT_FOLDER = ".data/queries"

#
# Functions
#

from os import makedirs, system
from os.path import abspath, join

import duckdb


#
# Main
#

query = r"""
    -- Todos estabelecimentos com determinado CNAE (primario ou secundario)
    SELECT 
        format('{:t.}/{:04d}-{:02d}', 100000000 + CAST(es.cnpj_base AS INTEGER), CAST(es.cnpj_ordem AS INTEGER), CAST(es.cnpj_dv AS INTEGER))[2:19] AS cnpj, 
        em.razao_social, 
        es.nome_fantasia, 
        es.matriz,
        
        -- Contato
        es.ddd1 || ' ' || es.telefone1 telefone1, 
        es.ddd2 || ' ' || es.telefone2 telefone2, 
        es.ddd_fax || ' ' || es.fax fax, 
        correio_eletronico email,
        mun.descricao municipio, 
        es.uf, 

        -- Cadastro
        em.capital_social,
        CASE em.porte_empresa
            WHEN 1 THEN 'Não informado'
            WHEN 2 THEN 'Micro empresa'
            WHEN 4 THEN 'Empresa de pequeno porte'
            WHEN 5 THEN 'Demais'
        ELSE
            '?'
        END AS porte_empresa,	
        es.cnae, 
        es.cnae_secundario,
        CASE es.situacao_cadastral
            WHEN 1 THEN 'Nula'
            WHEN 2 THEN 'Ativa'
            WHEN 3 THEN 'Suspensa'
            WHEN 4 THEN 'Inapta'
            WHEN 8 THEN 'Baixada'
        ELSE
            '?'
        END	AS situacao_cadastral,
        es.data_situacao_cadastral,
        es.motivo_situacao_cadastral,
        es.data_inicio_atividades,
        REPLACE(REPLACE(es.regime_tributacao, 'LUCRO ', ''), ' DO IRPJ', '') AS regime_tributacao,
        
        -- SIMPLES
        em.opcao_simples,
        em.data_opcao_simples,
        em.data_exclusao_simples,
        
        -- MEI
        em.opcao_mei,
        em.data_opcao_mei,
        em.data_exclusao_mei
    FROM 
        estabelecimento es
        JOIN empresa em ON em.cnpj_base = es.cnpj_base
        JOIN municipio mun ON mun.codigo = es.municipio      
    WHERE 
        (es.cnae = $Cnae OR CONTAINS(es.cnae_secundario, $Cnae))
    ORDER BY 1
"""

CNAE = "1113502"

makedirs(OUTPUT_FOLDER, exist_ok=True)
print("Querying database...")

db_file = join(INPUT_FOLDER, "rfb-cnpj.duckdb")
out_file = join(INPUT_FOLDER, "Q1_by_cnae.xlsx")

con = duckdb.connect(db_file)
df = con.query(query, params={"Cnae": CNAE}).pl()
df.write_excel(out_file, table_style="Table Style Light 9", autofit=True)

print(f"  Done. Output file is '{out_file}'.")
system(abspath(out_file))
