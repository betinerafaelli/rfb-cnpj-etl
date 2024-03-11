# rfb-cnpj-etl

Ferramentas para consulta da base nacional de CNPJ da RFB.

Disponível em https://dados.rfb.gov.br/CNPJ.



# Requisitos

- Python 3.11 
- Poetry: https://python-poetry.org
- 40 GB de espaço livre em disco
- 32 GB de RAM (recomendável)



# Uso

## Inicialização

A partir da pasta raiz do projeto, execute:

```bash
# Inicializa ambiente python
poetry install
poetry shell

# Baixa os dados da RFB e cria o banco de dados
python ./rfb_cnpj_etl/L0_download.py
python ./rfb_cnpj_etl/L1_extract.py
python ./rfb_cnpj_etl/L2_load.py
python ./rfb_cnpj_etl/L3_refine.py
```

Todos os dados necessários serão baixados ou gerados na pasta `.data/`.

Observações:
  - Os scripts de inicialização são idempotentes. 
    - Etapas já executadas não serão realizadas novamente.

  - Para as consultas, apenas os dados da última pasta (`data\L3-gold`) são necessários. 
    - Apague as demais pastas intermediárias para liberar espaço em disco.



## Consultas

Alguns exemplos de consultas (geram o resultado num arquivo `.xlsx`)

```bash
# Todas estabelecimentos com determinado CNAE.
python ./rfb_cnpj_etl/Q1_by_cnae.py

# Dados dos estabelecimentos por CNPJ.
python ./rfb_cnpj_etl/Q2_by_cnpj.py
```
