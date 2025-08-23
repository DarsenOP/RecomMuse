-- Oldest Songs 
SELECT 
  title,
  `year` 
FROM dfs.data.`music_data.avro`
WHERE `year` <> 0
ORDER BY `year` ASC
LIMIT 10;

-- Newest Songs
SELECT 
  title,
  `year` 
FROM dfs.data.`music_data.avro`
WHERE `year` <> 0
ORDER BY `year` DESC 
LIMIT 10;

-- Hottest songs that are the shortest and show highest energy with lowest tempo
SELECT 
  title,
  song_hotttnesss,
  duration,
  energy,
  tempo 
FROM dfs.data.`music_data.avro`
WHERE song_hotttnesss <> 'NaN'
ORDER BY
    song_hotttnesss DESC,
    duration ASC,
    energy DESC,
    tempo ASC
LIMIT 10;

-- Albums with the most tracks in them
SELECT 
    `release`,
    COUNT(`release`) AS ntrack 
FROM dfs.data.`music_data.avro`
GROUP BY `release`
ORDER BY ntrack DESC
LIMIT 10;

-- Name of the artists with the longest song
SELECT 
  artist_name,
  duration
FROM dfs.data.`music_data.avro`
ORDER BY duration DESC
LIMIT 10; 

-- Percentage of songs without a year  
SELECT 
    ROUND(COUNT(CASE WHEN `year` = 0 THEN 1 END) * 100.0 / COUNT(*), 2)
FROM dfs.data.`music_data.avro`
