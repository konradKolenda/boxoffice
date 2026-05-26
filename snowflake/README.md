# Obiekty w `RAW`

## `RAW.REVENUES` (tabela)

Standardowa tabela. Airflow wywołuje procedurę `SP_COPY_REVENUES_FROM_RAW`, która robi `COPY INTO` ze stage'a `S3_BOX_OFFICE` (jeden plik na uruchomienie DAG-a). Idempotencja na poziomie pliku.

Kolumny biznesowe (`ID`, `REVENUE_DATE`, `TITLE`, `REVENUE`, `THEATERS`, `DISTRIBUTOR`) + metadane EL (`_SOURCE_FILE` = `METADATA$FILENAME` z COPY, `_LOADED_AT` = `CURRENT_TIMESTAMP()`).

## `RAW.OMDB` (External Table)

Dane fizycznie żyją w `s3://kk-demo-pipeline/raw/omdb/` jako pliki JSON (jeden plik na pojedyncze odpytanie OMDb). Snowflake czyta je wprost przez stage `S3_OMDB` — żadnego COPY, żadnej kopii w warehousie.

Kolumny pochodne z VARIANT-owej kolumny `VALUE`:

| Kolumna | Wyrażenie | Po co |
|---|---|---|
| `LOOKUP_TITLE` | `VALUE:_lookup:title::VARCHAR` | klucz złączenia — co pytaliśmy OMDb (po normalizacji białych znaków) |
| `LOOKUP_YEAR` | `VALUE:_lookup:year::NUMBER` | klucz złączenia — `MIN(YEAR(revenue_date))` na tytuł |
| `STATUS` | `VALUE:_status::VARCHAR` | `'found'` lub `'not_found'` (błędy nigdy nie trafiają do S3) |
| `IMDB_ID` | `VALUE:imdbID::VARCHAR` | naturalny klucz z OMDb (`NULL` gdy `not_found`) |
| `RESPONSE` | `VALUE` | cały opakowany JSON (na wypadek ponownego użycia / dostęp do dowolnego pola) |
| `_FETCHED_AT` | `VALUE:_fetched_at::TIMESTAMP_NTZ` | metadana EL |

`AUTO_REFRESH = FALSE` — Airflow po pętli pobierania wywołuje `CALL SP_REFRESH_OMDB_CACHE()` żeby External Table dostrzegł nowe pliki.

## `RAW.OMDB_FETCH_LOG` (tabela)

Jeden wiersz na każde wywołanie OMDb **niezależnie od wyniku** — czy zwróciło `found`, `not_found`, czy padło (`error_5xx`, `error_429`, `error_timeout`, `error_other`).

Trzy zastosowania:

1. **Licznik zużytej quoty** — `SELECT COUNT(*) FROM OMDB_FETCH_LOG WHERE call_at::DATE = CURRENT_DATE()`. Każde wywołanie (sukces, brak, błąd) zużywa jeden z 1000 dziennych slotów OMDb, więc tu jest źródło prawdy.
2. **Pomijanie tytułów, które dziś już padły** — tytuły z dzisiejszym błędem są wycięte z kolejki, żeby nie zużywać quoty na powtarzanie tego samego.
3. **Audyt** — `WHERE outcome LIKE 'error%' GROUP BY ...` daje rozbicie problemów po stronie API.

## `RAW.OMDB_FETCH_QUEUE` (view)

Kolejka uzupełniania o dane z OMDb. Logika anti-join:

1. Z `RAW.REVENUES` agregat: unique `(title, MIN(YEAR(revenue_date)))` + `SUM(revenue)` jako `lifetime_rev` (priorytetyzacja).
2. `LEFT JOIN` do `RAW.OMDB` po `(lookup_title, lookup_year)` → `IS NULL` = nie ma w cache.
3. `LEFT JOIN` do dzisiejszych błędów w `OMDB_FETCH_LOG` → `IS NULL` = nie próbowaliśmy dziś.

Skrypt (`omdb_fetch.py`) wywołuje:

```sql
SELECT title, release_year FROM BOXOFFICE.RAW.OMDB_FETCH_QUEUE
ORDER BY lifetime_rev DESC NULLS LAST
LIMIT 950
```

Czyli: najpierw najbardziej dochodowe filmy, które jeszcze nie były uzupełnione o dane z OMDb API i nie padły dziś. `ORDER BY` po stronie skryptu — view Snowflake nie gwarantuje zachowania ORDER BY przy SELECT.
