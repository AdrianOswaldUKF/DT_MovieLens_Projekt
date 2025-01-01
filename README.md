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

**Autor:** Adrián Oswald

