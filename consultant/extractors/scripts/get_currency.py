import sys
import os


# Добавляем корневую директорию проекта в sys.path
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '../..')))

from consultant.extractors.base import download_cbr_currency_to


if __name__ == '__main__':
    download_cbr_currency_to('/user/b.kustov/raw_data/cbr')