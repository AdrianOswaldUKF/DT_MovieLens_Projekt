# DT_MovieLens_Projekt

---

# **ETL proces pre MovieLens dataset**

Tento repozitár obsahuje implementáciu ETL procesu v Snowflake na analýzu údajov z datasetu **MovieLens**. Projekt sa sústreďuje na preskúmanie správania používateľov a ich preferencií pri hodnotení filmov, pričom využíva demografické údaje. Výsledný dátový model umožňuje realizovať viacrozmernú analýzu a vizualizáciu kľúčových metrík.

---

## **1. Úvod a popis zdrojových údajov**

Primárnym cieľom tohto semestrálneho projektu je analyzovať údaje týkajúce sa filmov, používateľov a ich hodnotení. Táto analýza poskytuje prehľad o preferenciách, identifikuje najobľúbenejšie filmy a správanie jednotlivých používateľov.

Zdrojové dáta boli získané zo školského servera Moodle. Dataset je dostupný [tu](https://edu.ukf.sk/mod/folder/view.php?id=252867). Obsahuje osem hlavných tabuliek:

- `age_group`
- `genres`
- `genres_movies`
- `movies`
- `occupations`
- `ratings`
- `tags`
- `users`

Hlavným cieľom ETL procesu bolo tieto údaje vyčistiť, transformovať a pripraviť na pokročilé analytické úlohy.

---

### **1.1 Dátová architektúra**

### **ERD diagram**

Zdrojové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**:

<p align="center">
  <img src="https://github.com/AdrianOswaldUKF/DT_MovieLens_Projekt/blob/main/erd_schema.png?raw=true" alt="ERD Schema">
  <br>
  <em>Obrázok 1 Entitno-relačná schéma MovieLens</em>
</p>

---

## **2 Dimenzionálny model**

Navrhnutý bol **hviezdicový model (star schema)**, ktorý je optimalizovaný na efektívnu analýzu. Jeho centrálnym prvkom je faktová tabuľka **`fact_ratings`**, ktorá je prepojená s nasledujúcimi dimenziami:

- **`dim_tags`**: Obsahuje detaily o názvoch tagov
- **`dim_time`**: Obsahuje podrobnosti o čase a hodine vytvorenia hodnotenia
- **`dim_date`**: Poskytuje informácie o dátume (rok, mesiac, deň, deň v týždni)
- **`dim_users`**: Zaznamenáva informácie o používateľoch (vek, pohlavie, PSČ, veková skupina, povolanie)
- **`dim_genres`**: Obsahuje názvy žánrov
- **`dim_movies`**: Uchováva informácie o názvoch filmov a rokoch vydania

Štruktúra hviezdicového modelu je znázornená na diagrame nižšie. Tento diagram ilustruje prepojenia medzi faktovou tabuľkou a dimenziami, čím zjednodušuje pochopenie a implementáciu celého modelu.

<p align="center">
  <img src="https://github.com/AdrianOswaldUKF/DT_MovieLens_Projekt/blob/main/star_schema.png?raw=true" alt="Star Schema">
  <br>
  <em>Obrázok 2 Schéma hviezdy pre MovieLens</em>
</p>

---

## **3. ETL proces v Snowflake**
ETL proces pozostával z troch hlavných fáz: `extrahovanie` (Extract), `transformácia` (Transform) a `načítanie` (Load). Tento proces bol implementovaný v Snowflake s cieľom pripraviť zdrojové dáta zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.

---
### **3.1 Extract (Extrahovanie dát)**
Dáta zo zdrojového datasetu (formát `.csv`) boli najprv nahraté do Snowflake prostredníctvom interného stage úložiska s názvom `WOMBAT_MOVIELENS_STAGE`. Stage v Snowflake slúži ako dočasné úložisko na import alebo export dát. Vytvorenie stage bolo zabezpečené príkazom:

#### Príklad kódu:
```sql
CREATE OR REPLACE STAGE WOMBAT_MOVIELENS_STAGE;
```
Do stage boli následne nahraté súbory obsahujúce údaje o filmoch, používateľoch, hodnoteniach a tagoch. Dáta boli importované do staging tabuliek pomocou príkazu `COPY INTO`. Pre každú tabuľku sa použil podobný príkaz:

```sql
COPY INTO movies_staging
FROM @WOMBAT_MOVIELENS_STAGE/movies.csv
FILE_FORMAT = WOMBAT_MOVIELENS_CSV
```

V prípade nekonzistentných záznamov bol použitý parameter `ON_ERROR = 'CONTINUE'`, ktorý zabezpečil pokračovanie procesu bez prerušenia pri chybách.

---
### **3.2 Transform (Transformácia dát)**
V tejto fáze boli dáta zo staging tabuliek vyčistené, transformované a obohatené. Hlavným cieľom bolo pripraviť dimenzie a faktovú tabuľku, ktoré umožnia jednoduchú a efektívnu analýzu.

**Dimenzia tagov (dim_tags)**
```sql
CREATE TABLE dim_tags AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY "tags") AS ID,
    "tags"
FROM tags_staging;
```

**Dimenzia časov (dim_time)**
```sql
CREATE TABLE dim_time AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY EXTRACT(HOUR FROM rated_at)) AS ID,
    EXTRACT(HOUR FROM rated_at) AS hour,
    CASE 
        WHEN EXTRACT(HOUR FROM rated_at) < 12 THEN 'AM'
        ELSE 'PM'
    END AS am_pm
FROM ratings_staging;
```

**Dimenzia dátumov (dim_date)**
```sql
CREATE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(rated_at AS DATE)) AS ID,
    CAST(rated_at AS DATE) AS date,
    EXTRACT(DAY FROM rated_at) AS day,
    EXTRACT(DOW FROM rated_at) + 1 AS day_of_week,
    CASE EXTRACT(DOW FROM rated_at) + 1
        WHEN 1 THEN 'Pondelok'
        WHEN 2 THEN 'Utorok'
        WHEN 3 THEN 'Streda'
        WHEN 4 THEN 'Štvrtok'
        WHEN 5 THEN 'Piatok'
        WHEN 6 THEN 'Sobota'
        WHEN 7 THEN 'Nedeľa'
    END AS day_of_week_as_string,
    EXTRACT(MONTH FROM rated_at) AS month,
    EXTRACT(YEAR FROM rated_at) AS year,
    EXTRACT(QUARTER FROM rated_at) AS quarter
FROM ratings_staging;
```

**Dimenzia filmov (dim_movies)**
```sql
CREATE TABLE dim_movies AS
SELECT
    ROW_NUMBER() OVER (ORDER BY title) AS ID,
    title,
    release_year
FROM movies_staging
GROUP BY title, release_year;
```

**Dimenzia žánrov (dim_genres)**
```sql
CREATE TABLE dim_genres AS
SELECT
    ROW_NUMBER() OVER (ORDER BY name) AS ID,
    name
FROM genres_staging
GROUP BY name;
```

**Dimenzia používateľov (dim_users)**
```sql
CREATE TABLE dim_users AS
SELECT
    u.id AS ID,
    u.age,
    CASE 
        WHEN u.age < 18 THEN 'Under 18'
        WHEN u.age BETWEEN 18 AND 24 THEN '18-24'
        WHEN u.age BETWEEN 25 AND 34 THEN '25-34'
        WHEN u.age BETWEEN 35 AND 44 THEN '35-44'
        WHEN u.age BETWEEN 45 AND 54 THEN '45-54'
        WHEN u.age >= 55 THEN '55+'
        ELSE 'Unknown'
    END AS age_group_name,
    u.gender,
    u.zip_code,
    o.name AS occupations_name
FROM users_staging u
JOIN occupations_staging o ON u.occupation_id = o.id;
```

**Faktová tabuľka (fact_ratings)**
```sql
CREATE TABLE fact_ratings AS
SELECT 
    r.id AS ID,
    r.rating,
    u.ID AS dim_users_ID,
    t.ID AS dim_tags_ID,
    m.ID AS dim_movies_ID,
    g.ID AS dim_genres_ID,
    tm.ID AS dim_time_ID,
    dd.ID AS dim_date_ID
FROM ratings_staging r
JOIN dim_users u 
  ON r.user_id = u.ID
LEFT JOIN tags_staging ts 
  ON r.user_id = ts.user_id 
 AND r.movie_id = ts.movie_id
LEFT JOIN dim_tags t 
  ON ts.id = t.ID
JOIN genres_movies_staging gm 
  ON r.movie_id = gm.movie_id
JOIN dim_genres g 
  ON gm.genre_id = g.ID
JOIN dim_time tm 
  ON EXTRACT(HOUR FROM r.rated_at) = tm.hour
 AND (CASE 
        WHEN EXTRACT(HOUR FROM r.rated_at) < 12 THEN 'AM'
        ELSE 'PM'
      END
     ) = tm.am_pm
JOIN dim_date dd 
  ON CAST(r.rated_at AS DATE) = dd.date;
```
---
### **3.3 Load (Načítanie dát)**
Po úspešnom vytvorení dimenzií a faktovej tabuľky boli dáta nahraté do finálnej štruktúry. Na záver boli staging tabuľky odstránené, aby sa optimalizovalo využitie úložiska:
```sql
DROP TABLE IF EXISTS age_group_staging;
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS tags_staging;
DROP TABLE IF EXISTS users_staging;
```
ETL proces v Snowflake umožnil spracovanie pôvodných dát z `.csv` formátu do viacdimenzionálneho modelu typu hviezda. Tento proces zahŕňal čistenie, obohacovanie a reorganizáciu údajov. Výsledný model umožňuje analýzu preferencií a správania používateľov, pričom poskytuje základ pre vizualizácie a reporty.

---

## **4 Vizualizácia dát**

Dashboard obsahuje `5 vizualizácií`, ktoré poskytujú detailný prehľad o kľúčových ukazovateľoch a trendoch týkajúcich sa filmov, používateľov a ich hodnotení. Tieto vizualizácie odpovedajú na dôležité otázky a umožňujú lepšie pochopiť správanie divákov a ich preferencie.

<p align="center">
  <img src="https://github.com/AdrianOswaldUKF/DT_MovieLens_Projekt/blob/main/movielens_dashboard.png?raw=true" alt="ERD Schema">
  <br>
  <em>Obrázok 3 Dashboard MovieLens datasetu</em>
</p>

---
### **Graf 1: Časové rozloženie hodnotení**
Táto vizualizácia zobrazuje počet hodnotení podľa hodiny dňa a rozlíšenia na dopoludnie (AM) a popoludnie (PM). Umožňuje analyzovať, kedy sú používatelia najaktívnejší pri hodnotení filmov. Dáta môžu byť užitočné na plánovanie marketingových kampaní alebo časovania odporúčaní filmov.

```sql
SELECT 
    t.hour, 
    t.am_pm, 
    COUNT(r.ID) AS rating_count
FROM fact_ratings r
JOIN dim_time t ON r.dim_time_ID = t.ID
GROUP BY t.hour, t.am_pm
ORDER BY t.hour, t.am_pm;
```
---
### **Graf 2: Priemerné hodnotenie podľa vekových skupín**
Graf znázorňuje priemerné hodnotenia filmov podľa vekových skupín používateľov. Z údajov možno zistiť, ako sa hodnotiace preferencie líšia medzi mladšími a staršími skupinami divákov. Tento pohľad môže pomôcť pri cielení odporúčaní na základe demografických údajov.

```sql
SELECT 
    u.age_group_name, 
    ROUND(AVG(r.rating), 2) AS avg_rating
FROM fact_ratings r
JOIN dim_users u ON r.dim_users_ID = u.ID
GROUP BY u.age_group_name
ORDER BY avg_rating DESC;
```
---
### **Graf 3: Percentuálne zastúpenie žánrov vo všetkých hodnoteniach**
Vizualizácia ukazuje rozdelenie hodnotení podľa žánrov a poskytuje pohľad na preferované kategórie filmov. Pomocou tohto grafu možno identifikovať dominantné žánre a analyzovať, ktoré typy filmov sú medzi používateľmi najobľúbenejšie.

```sql
SELECT 
    g.name AS genre, 
    ROUND(COUNT(r.ID) * 100.0 / SUM(COUNT(r.ID)) OVER (), 2) AS percentage
FROM fact_ratings r
JOIN dim_genres g ON r.dim_genres_ID = g.ID
GROUP BY g.name
ORDER BY percentage DESC;
```
---
### **Graf 4: Aktivita hodnotení podľa dní v týždni a sezónnych trendov**
Graf zobrazuje počet hodnotení počas jednotlivých dní v týždni a zároveň ukazuje sezónne trendy podľa štvrťrokov. Tento pohľad odhaľuje, kedy sú používatelia najaktívnejší a aké sú rozdiely medzi rôznymi obdobiami roka, čo môže pomôcť pri plánovaní propagačných akcií.

```sql
SELECT 
    d.day_of_week_as_string, 
    EXTRACT(QUARTER FROM d.date) AS quarter,
    COUNT(r.ID) AS rating_count
FROM fact_ratings r
JOIN dim_date d ON r.dim_date_ID = d.ID
GROUP BY d.day_of_week_as_string, EXTRACT(QUARTER FROM d.date)
ORDER BY quarter, rating_count DESC;
```
---
### **Graf 5: Rozloženie hodnotení podľa dĺžky názvov filmov**
Táto vizualizácia analyzuje počet hodnotení na základe dĺžky názvov filmov. Pomáha identifikovať, či existuje súvislosť medzi dĺžkou názvu filmu a jeho popularitou, čo môže byť zaujímavé pre marketing a branding.

```sql
SELECT 
    LENGTH(m.title) AS title_length, 
    COUNT(r.ID) AS rating_count
FROM fact_ratings r
JOIN dim_movies m ON r.dim_movies_ID = m.ID
GROUP BY LENGTH(m.title)
ORDER BY title_length;
```
---

Dashboard poskytuje komplexný pohľad na údaje o filmoch, hodnoteniach a používateľoch. Tieto vizualizácie umožňujú identifikovať kľúčové trendy a správanie divákov, čo môže byť využité na optimalizáciu odporúčacích systémov, marketingových stratégií a zlepšenie užívateľského pocitu.

---
**Autor:** Adrián Oswald
