#
# L1: Extrai os arquivos .csv e converte de 'latin1' para 'utf-8'. (19.6 GB, ~3 min)
#

INPUT_FOLDER = ".data/L0-zip"
OUTPUT_FOLDER = ".data/L1-csv"


#
# Functions
#

from glob import glob
from io import TextIOWrapper
from os import makedirs
from os.path import isfile, join
from shutil import copyfileobj
from zipfile import ZipFile


def extract_files(input_files, output_path):
    file_count = 0
    for zip_file in input_files:
        with ZipFile(zip_file, "r") as zip_ref:
            names = zip_ref.namelist()
            for n in names:
                member = zip_ref.getinfo(n)

                target_file = join(output_path, member.filename)
                if not isfile(target_file):
                    with zip_ref.open(member) as s:
                        with open(target_file, "w", encoding="utf-8") as t:
                            copyfileobj(TextIOWrapper(s, encoding="latin1"), t)

                print(f"    {target_file}")
                file_count += 1

    return file_count


#
# Main
#


def main():
    makedirs(OUTPUT_FOLDER, exist_ok=True)
    print("L1: Extracting files...")

    all_zip_files = glob(join(INPUT_FOLDER, "*.zip"))
    file_count = extract_files(all_zip_files, OUTPUT_FOLDER)

    print(f"L1: {file_count} files ready.")


if __name__ == "__main__":
    main()
