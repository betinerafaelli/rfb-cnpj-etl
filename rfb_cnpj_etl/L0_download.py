#
# L0: Baixa os arquivos .zip do site da RFB. (5.3 GB, ~10 min)
#

ROOT_URL = "https://dados.rfb.gov.br/CNPJ/"
OUTPUT_FOLDER = ".data/L0-zip"

# Todos arquivos a baixar (exceto Socios) -- Mar/2024
SOURCE_FILES = [
    "Cnaes.zip",
    "Empresas0.zip",
    "Empresas1.zip",
    "Empresas2.zip",
    "Empresas3.zip",
    "Empresas4.zip",
    "Empresas5.zip",
    "Empresas6.zip",
    "Empresas7.zip",
    "Empresas8.zip",
    "Empresas9.zip",
    "Estabelecimentos0.zip",
    "Estabelecimentos1.zip",
    "Estabelecimentos2.zip",
    "Estabelecimentos3.zip",
    "Estabelecimentos4.zip",
    "Estabelecimentos5.zip",
    "Estabelecimentos6.zip",
    "Estabelecimentos7.zip",
    "Estabelecimentos8.zip",
    "Estabelecimentos9.zip",
    "Motivos.zip",
    "Municipios.zip",
    "Naturezas.zip",
    "Paises.zip",
    "Qualificacoes.zip",
    "Simples.zip",
    "regime_tributario/Imunes e isentas.zip",
    "regime_tributario/Lucro Arbitrado.zip",
    "regime_tributario/Lucro Presumido 1.zip",
    "regime_tributario/Lucro Real.zip",
]


#
# Functions
#

from os import makedirs
from os.path import basename, getsize, isfile, join
from shutil import move
from tempfile import NamedTemporaryFile
from urllib.parse import urljoin

from httpx import AsyncClient
from tqdm import tqdm

import asyncio


# asyncio.gather limited to 'n' concurrent tasks -- https://stackoverflow.com/a/61478547/33244
async def gather_with_semaphore(n, *tasks):
    semaphore = asyncio.Semaphore(n)

    async def wrapped_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(wrapped_task(c) for c in tasks))


# Download multiple files concurrently
async def download_files_async(root_url, files, output_path):
    BAR_FORMAT = "{desc: <25} {percentage:3.0f}% {bar} [{remaining}, {rate_fmt}]"
    MAX_CONCURRENT_DOWNLOADS = 8

    async def download_file_async(root_url, file_name, target_path, position):
        full_url = urljoin(root_url, file_name)
        full_target_file = join(target_path, basename(file_name))
        if isfile(full_target_file):
            # File already exists: Just update progress to 100%.
            total = getsize(full_target_file)
            with tqdm(
                desc=f"  {file_name}",
                total=total,
                leave=False,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                bar_format=BAR_FORMAT,
                position=position,
                dynamic_ncols=True,
            ) as progress:
                progress.update(total)
            return

        with NamedTemporaryFile(delete=False) as temp_file:
            async with AsyncClient() as client:
                async with client.stream("GET", full_url) as response:
                    total = int(response.headers["Content-Length"])

                    with tqdm(
                        desc=f"  {file_name}",
                        total=total,
                        leave=False,
                        unit="B",
                        unit_scale=True,
                        unit_divisor=1024,
                        bar_format=BAR_FORMAT,
                        position=position,
                        dynamic_ncols=True,
                    ) as progress:
                        num_bytes_downloaded = response.num_bytes_downloaded
                        async for chunk in response.aiter_bytes():
                            temp_file.write(chunk)
                            progress.update(response.num_bytes_downloaded - num_bytes_downloaded)
                            num_bytes_downloaded = response.num_bytes_downloaded

        move(temp_file.name, full_target_file)

    tasks = [download_file_async(root_url, file, output_path, i) for i, file in enumerate(files)]
    await gather_with_semaphore(MAX_CONCURRENT_DOWNLOADS, *tasks)
    return len(files)


#
# Main
#


async def main_async():
    makedirs(OUTPUT_FOLDER, exist_ok=True)
    print("L0: Downloading files...")

    file_count = await download_files_async(ROOT_URL, SOURCE_FILES, OUTPUT_FOLDER)

    print(f"\rL0: {file_count} files ready.")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
