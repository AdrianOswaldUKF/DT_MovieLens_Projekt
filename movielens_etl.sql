-- 3.1 Extract

CREATE OR REPLACE STAGE WOMBAT_MOVIELENS_STAGE;

CREATE OR REPLACE FILE FORMAT WOMBAT_MOVIELENS_CSV
TYPE = CSV
COMPRESSION = NONE
FIELD_DELIMITER = ','
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
FILE_EXTENSION = 'csv'
SKIP_HEADER = 1
RECORD_DELIMITER = '\n'
TRIM_SPACE = FALSE
NULL_IF = ('NULL', 'null', '');

-- Vytvorenie tabuľky vekových skupín
CREATE TABLE age_group_staging(
    id INT PRIMARY KEY,
    name VARCHAR(45)
);

-- Vyvtvorenie tabuľky žánrov
CREATE TABLE genres_staging(
    id INT PRIMARY KEY,
    name VARCHAR(255)
);

-- Vytvorenie tabuľky filmov
CREATE TABLE movies_staging(
    id INT PRIMARY KEY,
    title VARCHAR(255),
    release_year CHAR(4)
);

-- Vytvorenie tabuľky zamestnaní
CREATE TABLE occupations_staging(
    id INT PRIMARY KEY,
    name VARCHAR(255)
);

-- Vytvorenie tabuľky používateľov
CREATE TABLE users_staging(
    id INT PRIMARY KEY,
    age INT,
    gender CHAR(1),
    occupation_id INT,
    zip_code VARCHAR(255),
    FOREIGN KEY (occupation_id) REFERENCES occupations_staging(id)
);

-- Vytvorenie tabuľky tagov 
CREATE TABLE tags_staging(
    id INT PRIMARY KEY,
    user_id INT,
    movie_id INT,
    "tags" VARCHAR(4000),
    created_at DATETIME,
    FOREIGN KEY (user_id) REFERENCES users_staging(id),
    FOREIGN KEY (movie_id) REFERENCES movies_staging(id)
);

-- Vytvorenie tabuľky hodnotení
CREATE TABLE ratings_staging(
    id INT PRIMARY KEY,
    user_id INT,
    movie_id INT,
    rating INT,
    rated_at TIMESTAMP_NTZ(9),
    FOREIGN KEY (user_id) REFERENCES users_staging(id),
    FOREIGN KEY (movie_id) REFERENCES movies_staging(id)
);

-- Vytvorenie spojovacej tabuľky medzi tabuľkami žánrov a filmov
CREATE TABLE genres_movies_staging(
    id INT PRIMARY KEY,
    movie_id INT,
    genre_id INT,
    FOREIGN KEY (movie_id) REFERENCES movies_staging(id),
    FOREIGN KEY (genre_id) REFERENCES genres_staging(id)
);

-- Nahranie dát do tabuľky vekových skupín
COPY INTO age_group_staging
FROM @WOMBAT_MOVIELENS_STAGE/age_group.csv
FILE_FORMAT = WOMBAT_MOVIELENS_CSV
ON_ERROR = 'CONTINUE';

-- Nahranie dát do tabuľky žánrov
COPY INTO genres_staging
FROM @WOMBAT_MOVIELENS_STAGE/genres.csv
FILE_FORMAT = WOMBAT_MOVIELENS_CSV
ON_ERROR = 'CONTINUE';

-- Nahranie dát do tabuľky filmov
COPY INTO movies_staging
FROM @WOMBAT_MOVIELENS_STAGE/movies.csv
FILE_FORMAT = WOMBAT_MOVIELENS_CSV
ON_ERROR = 'CONTINUE';

-- Nahranie dát do tabuľky zamestnaní
COPY INTO occupations_staging
FROM @WOMBAT_MOVIELENS_STAGE/occupations.csv
FILE_FORMAT = WOMBAT_MOVIELENS_CSV
ON_ERROR = 'CONTINUE';

-- Nahranie dát do tabuľky použivateľov
COPY INTO users_staging 
FROM @WOMBAT_MOVIELENS_STAGE/users.csv
FILE_FORMAT = WOMBAT_MOVIELENS_CSV
ON_ERROR = 'CONTINUE';

-- Nahranie dát do tabuľky tagov
COPY INTO tags_staging
FROM @WOMBAT_MOVIELENS_STAGE/tags.csv
FILE_FORMAT = WOMBAT_MOVIELENS_CSV
ON_ERROR = 'CONTINUE';

-- Nahranie dát do tabuľky ratingov
COPY INTO ratings_staging
FROM @WOMBAT_MOVIELENS_STAGE/ratings.csv
FILE_FORMAT = WOMBAT_MOVIELENS_CSV
ON_ERROR = 'CONTINUE';


-- Nahranie dát do spojovacej tabuľky žánrov a filmov
COPY INTO genres_movies_staging
FROM @WOMBAT_MOVIELENS_STAGE/genres_movies.csv
FILE_FORMAT = WOMBAT_MOVIELENS_CSV
ON_ERROR = 'CONTINUE';

-- 3.2 Transform

-- Dimenzia tagov
CREATE TABLE dim_tags AS
SELECT
    ROW_NUMBER() OVER (ORDER BY "tags") AS ID,
    "tags"
FROM tags_staging
GROUP BY "tags";

-- Dimenzia časov
CREATE OR REPLACE TABLE dim_time AS
SELECT
    ROW_NUMBER() OVER (ORDER BY rated_at) AS ID,
    EXTRACT(HOUR FROM rated_at) AS hour,
    CASE 
        WHEN EXTRACT(HOUR FROM rated_at) < 12 THEN 'AM'
        ELSE 'PM'
    END AS am_pm
FROM ratings_staging
GROUP BY hour, am_pm
ORDER BY hour, am_pm;

-- Dimenzia dátumov
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
FROM ratings_staging
GROUP BY 
    CAST(rated_at AS DATE),
    EXTRACT(DAY FROM rated_at),
    EXTRACT(DOW FROM rated_at),
    EXTRACT(MONTH FROM rated_at),
    EXTRACT(YEAR FROM rated_at),
    EXTRACT(QUARTER FROM rated_at);



-- Dimenzia filmov
CREATE TABLE dim_movies AS
SELECT
    ROW_NUMBER() OVER (ORDER BY title) AS ID,
    title,
    release_year
FROM movies_staging
GROUP BY title, release_year;

-- Dimenzia žánrov
CREATE TABLE dim_genres AS
SELECT
    ROW_NUMBER() OVER (ORDER BY name) AS ID,
    name
FROM genres_staging
GROUP BY name;

-- Dimenzia používateľov
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

-- Tabuľka fakt_ratings
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
-- Pripojenie používateľov
JOIN dim_users u 
  ON r.user_id = u.ID
-- Pripojenie filmov
JOIN dim_movies m 
  ON r.movie_id = m.ID
-- Pripojenie tagov cez ID, ak je k dispozícii
LEFT JOIN tags_staging ts 
  ON r.user_id = ts.user_id 
 AND r.movie_id = ts.movie_id
LEFT JOIN dim_tags t 
  ON ts.id = t.ID
-- Pripojenie žánrov
JOIN genres_movies_staging gm 
  ON r.movie_id = gm.movie_id
JOIN dim_genres g 
  ON gm.genre_id = g.ID
-- Pripojenie času
JOIN dim_time tm 
  ON EXTRACT(HOUR FROM r.rated_at) = tm.hour
 AND (CASE 
        WHEN EXTRACT(HOUR FROM r.rated_at) < 12 THEN 'AM'
        ELSE 'PM'
      END
     ) = tm.am_pm
-- Pripojenie dátumu
JOIN dim_date dd 
  ON CAST(r.rated_at AS DATE) = dd.date;


-- 3.3 Load

-- DROP staging
DROP TABLE IF EXISTS AGE_GROUP_STAGING;
DROP TABLE IF EXISTS GENRES_MOVIES_STAGING;
DROP TABLE IF EXISTS GENRES_STAGING;
DROP TABLE IF EXISTS MOVIES STAGING;
DROP TABLE IF EXISTS OCCUPATIONS_STAGING;
DROP TABLE IF EXISTS RATINGS_STAGING;
DROP TABLE IF EXISTS TAGS_STAGING;
DROP TABLE IF EXISTS USERS_STAGING;
