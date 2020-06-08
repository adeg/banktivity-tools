# Banktivity Importers

This is a collection of various tools (at the moment of writing: 2) to work with
data in the Banktivity storage backend (Apple Core Data using SQLite storage).

Note that this is meant for those who are familiar with Python to at least some
extent. THIS IS NOT A COMPLETE PRODUCT. It is provided as-is. Take necessary
precautions: backup your data, work on test copies and make sure to verify the
results, do a couple calculations manually to make sure transactions and totals
were correctly imported.

Table of Contents
=================
* importer-tinkoff-api.py
* migrate-acemoney.py

Tinkoff Investments OpenAPI Importer
====================================

Installation
------------
1. Install the libraries
  * Python keyring library, which provides an easy way to access the
  system keyring service from Python. Refer to the project page for details:
  https://pypi.org/project/keyring/

  ```bash
  $ pip3 install keyring
  ```

  * Tinkoff Investments OpenAPI Python library (one of the unofficial ones,
  more details at the link https://github.com/Awethon/open-api-python-client/):

  ```bash
  $ pip3 install -i https://test.pypi.org/simple/ --extra-index-url=https://pypi.org/simple/ tinkoff-invest-openapi-client
  ```

2. Store the Tinkoff Investments OpenAPI token securely in whatever keyring
backend your system is using and the keyring library supports:

  ```bash
  $ keyring set adeg/banktivity-importer tinkoff-api
  Password for 'tinkoff-api' in 'adeg/banktivity-importer': <paste the token and press Enter>
  ```
  (optional) Verify that the token has been successfully stored:
  ```bash
  $ keyring get adeg/banktivity-importer tinkoff-api
  t.[redacted]
  ```

3. Adjust settings in `settings.ini`

4. Run the importer:

  ```bash
  $ ./importer-tinkoff-api.py import all '2020-01-01 00:00:00' '2020-06-30 23:59:59' ~/Documents/banktivity-document.bank7
  ```


Tinkoff Investments OpenAPI importer caveats
--------------------------------------------
These are in Russian as if you are using Tinkoff broker you most likely speak
Russian :-)
* Тинькофф.Инвестиции удерживает НДФЛ с купонного дохода по долларовым бумагам
с рублевого счета, но GUI Banktivity не умеет создавать проводки в одной валюте
с привязкой к бумаге (Security) в другой валюте (интерфейс 7.5.1 предлагал
создать новую бумагу в рублях). Но в базе такие проводки создаются и не
замечено, что они создают проблемы. Просто хочу, чтобы вы учитывали данную
особенность, если пользуетесь моим методом отражения удержаний НДФЛ с
привязкой к бумаге (а не просто списаниям).
* ETF operations are not exported by Tinkoff at all, so all these have to be
exported manually.
* Bond par values are not imported as Tinkoff does not appear to expose this,
so all bonds imported from Tinkoff will have their par value set to 1000.



AceMoney Importer
=================
Import everything (well, almost) from AceMoney XML export. Coming in the
following commits.
