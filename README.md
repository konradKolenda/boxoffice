# boxoffice-pipeline

Demo data pipeline. Symuluje proces otrzymywania plików źródłowych (w tym przypadku CSV) z dziennymi przychodami z box-office. Pliki wrzucane są na S3, uzupełniane o metadane z OMDb API, by finalnie wylądować w Snowflake gdzie za pomocą dbt je testujemy i budujemy model danych. Model ten konsumowany jest przez raport w Power BI Service. Wszystko orkiestrowane przez Airflow na Astronomer.

## Stack i kluczowe decyzje

Skoncentrowałem się na części EL i muszę przyznać, że zadanie okazało się naprawdę ciekawe — nie tyle pod kątem samego wykonania, co optymalizacji.

- **Snowflake** — oprócz raczej typowej struktury myślę, że punktem wartym omówienia jest użycie External Table oraz procedur do ładowania danych.
- **dbt** — warto zwrócić uwagę, że to w tej warstwie sprawdzam duplikaty.
- **Airflow** (Astronomer) — jeden DAG, 4 taski w łańcuchu: `raw_data_ingestion` → `omdb_api_enrichment` → `dbt_build` → `refresh_pbi`.
- **Power BI** — uczciwie przyznaję, że wyszedłem z założenia, że wizualna część raportu nie jest tutaj punktem najważniejszym, dlatego czas głównie poświęciłem na część EL całego procesu.

## How it works

```mermaid
%%{init: {'flowchart': {'curve': 'linear'}}}%%
flowchart TB
    Client["Client CSV<br/>(simulated)"] --> S3In["S3 inbox/"]
    S3In -->|"Airflow pickup"| SfRev["Snowflake<br/>RAW.REVENUES"]
    OMDb["OMDb API<br/>(1000 calls/day)"] -->|"Airflow fetch"| S3Omdb["S3 raw/omdb/<br/>(JSON cache)"]
    S3Omdb -->|"External Table"| SfOmdb["Snowflake<br/>RAW.OMDB"]
    SfRev --> Dbt{"dbt build"}
    SfOmdb --> Dbt
    Dbt --> Marts["Snowflake MARTS<br/>(dims + fact)"]
    Marts -->|"DirectQuery"| PBI["Power BI Service"]
    Astro["Astronomer / Airflow<br/>(orchestrator)"] -.-> S3In
    Astro -.-> OMDb
    Astro -.-> Dbt
    Astro -.-> PBI
    classDef orch fill:#11213a,stroke:#5b9bd5,color:#e8eef7;
    class Astro orch;
```

## Co sprawdziłem przed rozpoczęciem i co miało wpływ na finalną strukturę

**Data quality**

![OMDb not_found analysis](assets/not_found.png)

<details><summary>Multi-year films — pokaż screen</summary>

![Multi-year films analysis](assets/multi_year_films.png)

</details>

<details><summary>Title collisions — pokaż screen</summary>

![Title collisions analysis](assets/title_collisions.png)

</details>

**Performance**

![Performance benchmark](assets/perf.png)

## Założenia jakie przyjąłem

- **Darmowy plan OMDb to 1000 wywołań/dzień.** Skrypt pobierający trzyma licznik w `OMDB_FETCH_LOG` z buforem 50 wywołań na ponowne próby i ręczne testy.
- **Finalny odbiorca chce widzieć w raporcie tylko rekordy uzupełnione o dane z OMDb API.**
- **Klient przysyła dane za kwartał** — dlatego tak właśnie podzieliłem plik źródłowy (`data_YYYYq{1-4}.csv.gz`).

## Jak to wygląda w praktyce

Pełen przebieg pipeline'u w wideo:

| End-to-end demo | Logs & observability |
|:---:|:---:|
| [![End-to-end demo](https://img.youtube.com/vi/wKI8-XWdvwA/0.jpg)](https://youtu.be/wKI8-XWdvwA) | [![Logs walkthrough](https://img.youtube.com/vi/V4gAZ_OnRz8/0.jpg)](https://youtu.be/V4gAZ_OnRz8) |

## Struktura S3

```
s3://kk-demo-pipeline/
├── inbox/                                  # symulowane wrzucenie pliku przez klienta
│   └── box_office/
│       └── year=YYYY/
│           └── data_YYYYq{1-4}.csv.gz      # podział kwartalny (założenie: klient przysyła raz na kwartał)
│
├── raw/                                    # tu wskazują stage'e Snowflake
│   ├── box_office/                         # CSV w trakcie COPY INTO (pusty między uruchomieniami)
│   │   └── <file>.csv.gz
│   └── omdb/                               # cache JSONów z OMDb, partycjonowanie wg daty pobrania
│       └── yyyy=YYYY/mm=MM/dd=DD/
│           └── <imdbID>.json
│
└── archive/                                # audyt po załadowaniu
    └── box_office/
        └── YYYY-MM-DD/                     # data uruchomienia ingestion
            └── <file>.csv.gz
```

| Prefix | Właściciel | Czytane przez | Cykl życia |
|---|---|---|---|
| `inbox/` | "klient" (symulacja) | Airflow `raw_data_ingestion` | plik usuwany po przeniesieniu do `raw/` |
| `raw/box_office/` | Airflow | Snowflake stage `S3_BOX_OFFICE` | plik usuwany po przeniesieniu do `archive/` |
| `raw/omdb/` | Airflow `omdb_api_enrichment` | Snowflake stage `S3_OMDB` (External Table) | append-only, nigdy nie kasowane |
| `archive/box_office/` | Airflow | nikt (audyt / odtworzenie) | append-only |

## ER model

```mermaid
erDiagram
    DIM_MOVIE ||--o{ FACT_DAILY_REVENUE : "movie_key"
    DIM_DATE ||--o{ FACT_DAILY_REVENUE : "date_key"
    DIM_DISTRIBUTOR ||--o{ FACT_DAILY_REVENUE : "distributor_key"

    DIM_MOVIE {
        varchar imdb_id PK "raw.omdb.imdb_id"
        varchar title
        varchar lookup_title
        number release_year
        varchar director
        varchar genres
        number imdb_rating
        number rotten_tomatoes_score
        date first_revenue_date
        date last_revenue_date
        number lifetime_revenue
    }

    DIM_DATE {
        number date_key PK "raw.revenues.revenue_date jako YYYYMMDD"
        date date
        number year
        number quarter
        number month
        boolean is_weekend
    }

    DIM_DISTRIBUTOR {
        varchar distributor_key PK "MD5(raw.revenues.distributor)"
        varchar distributor_name
        number films_distributed_n
        number lifetime_revenue
    }

    FACT_DAILY_REVENUE {
        varchar source_row_id PK "raw.revenues.id"
        varchar movie_key FK "dim_movie.imdb_id"
        number date_key FK "dim_date.date_key"
        varchar distributor_key FK "dim_distributor.distributor_key"
        number revenue
        number theaters
        date revenue_date
    }
```

## Playground

[**pipeline.konradkolenda.dev**](https://pipeline.konradkolenda.dev/)

Zdaję sobie sprawę, że szeroko rozumiany frontend (czy też web development) nie jest czymś koniecznym w pracy data engineera, ale próba jego stworzenia okazała się dla mnie na tyle interesującym (i rozwijającym) zadaniem, że postanowiłem poświęcić na to czas.
