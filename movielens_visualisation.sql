-- 4. Vizualizácia dát

-- Graf 1: Rozloženie hodnotení podľa času (Heatmapa)
SELECT 
    t.hour, 
    t.am_pm, 
    COUNT(r.ID) AS rating_count
FROM fact_ratings r
JOIN dim_time t ON r.dim_time_ID = t.ID
GROUP BY t.hour, t.am_pm
ORDER BY t.hour, t.am_pm;

-- Graf 2: Priemerné hodnotenie filmov podľa vekových skupín
SELECT 
    u.age_group_name, 
    ROUND(AVG(r.rating), 2) AS avg_rating
FROM fact_ratings r
JOIN dim_users u ON r.dim_users_ID = u.ID
GROUP BY u.age_group_name
ORDER BY avg_rating DESC;

-- Graf 3: Percentuálne zastúpenie žánrov vo všetkých hodnoteniach
SELECT 
    g.name AS genre, 
    ROUND(COUNT(r.ID) * 100.0 / SUM(COUNT(r.ID)) OVER (), 2) AS percentage
FROM fact_ratings r
JOIN dim_genres g ON r.dim_genres_ID = g.ID
GROUP BY g.name
ORDER BY percentage DESC;

-- Graf 4: Najpopulárnejšie dni v týždni na hodnotenie filmov a sezónne trendy
SELECT 
    d.day_of_week_as_string, 
    EXTRACT(QUARTER FROM d.date) AS quarter,
    COUNT(r.ID) AS rating_count
FROM fact_ratings r
JOIN dim_date d ON r.dim_date_ID = d.ID
GROUP BY d.day_of_week_as_string, EXTRACT(QUARTER FROM d.date)
ORDER BY quarter, rating_count DESC;

-- Graf 5: Rozloženie hodnotení podľa dĺžky názvov filmov
SELECT 
    LENGTH(m.title) AS title_length, 
    COUNT(r.ID) AS rating_count
FROM fact_ratings r
JOIN dim_movies m ON r.dim_movies_ID = m.ID
GROUP BY LENGTH(m.title)
ORDER BY title_length;