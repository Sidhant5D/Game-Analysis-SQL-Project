create database game_analysis ;

use game_analysis;

select * from  level_details2;
select * from player_details;

# 1. Extract `P_ID`, `Dev_ID`, `PName`, and `Difficulty_level` of all players at Level 0.

SELECT pd.P_ID, ld.Dev_ID, pd.PName, ld.Level ,ld.difficulty AS Difficulty_level
FROM player_details pd
JOIN level_details2 ld ON pd.P_ID = ld.P_ID
WHERE ld.level = 0;

# 2. Find `Level1_code`wise average `Kill_Count` where `lives_earned` is 2, and at least 3 
# stages are crossed

SELECT pd.L1_code, AVG(ld2.kill_count) AS avg_kill_count
FROM player_details pd
JOIN level_details2 ld2 ON pd.P_ID = ld2.P_ID
WHERE ld2.lives_earned = 2
  AND ld2.stages_crossed >= 3
GROUP BY pd.L1_code;

# 3. Find the total number of stages crossed at each difficulty level for Level 2 with players 
# using `zm_series` devices. Arrange the result in decreasing order of the total number of 
# stages crossed.

SELECT ld.Dev_ID, ld.difficulty, SUM(ld.stages_crossed) AS total_stages_crossed
FROM level_details2 ld
JOIN Player_Details pd ON ld.P_ID = pd.P_ID
WHERE ld.Level = 'Level 2'
AND ld.Dev_ID LIKE '%zm_%' -- corrected the LIKE condition
GROUP BY ld.Dev_ID, ld.difficulty -- added ld.Dev_ID to GROUP BY
ORDER BY total_stages_crossed DESC;

# 4. Extract `P_ID` and the total number of unique dates for those players who have played 
# games on multiple days.

SELECT P_ID, COUNT(DISTINCT DATE(TimeStamp)) AS unique_dates
FROM level_details2
GROUP BY P_ID
HAVING COUNT(DISTINCT DATE(TimeStamp)) > 1;

# 5. Find `P_ID` and levelwise sum of `kill_counts` where `kill_count` is greater than the 
# average kill count for Medium difficulty.

SELECT ld.P_ID, ld.level, SUM(ld.kill_count) AS total_kill_count
FROM level_details2 ld
JOIN (
    SELECT difficulty, AVG(kill_count) AS avg_kill_count
    FROM level_details2
    WHERE difficulty = 'Medium'
    GROUP BY difficulty
) AS avg_kills ON ld.difficulty = avg_kills.difficulty
WHERE ld.kill_count > avg_kills.avg_kill_count
GROUP BY ld.P_ID, ld.level;

# 6.  Find `Level` and its corresponding `Level_code`wise sum of lives earned, excluding Level 
# 0. Arrange in ascending order of level.

SELECT Level , SUM(lives_earned) AS total_lives_earned
FROM level_details2
WHERE Level != 0
GROUP BY Level
ORDER BY Level ASC;

# 7. Find the top 3 scores based on each `Dev_ID` and rank them in increasing order using 
# `Row_Number`. Display the difficulty as well.

WITH RankedScores AS (
    SELECT 
        Dev_ID,
        difficulty,
        score,
        ROW_NUMBER() OVER (PARTITION BY Dev_ID ORDER BY score ASC) AS ranking
    FROM level_Details2
)
SELECT 
    Dev_ID,
    difficulty,
    score,
    ranking
FROM RankedScores
WHERE  ranking <= 3
limit 3;

# 8. Find the `first_login` datetime for each device ID.

SELECT Dev_ID, MIN(timestamp) AS first_login
FROM level_Details2
GROUP BY Dev_ID;

# 9. Find the top 5 scores based on each difficulty level and rank them in increasing order 
# using `Rank`. Display `Dev_ID` as well

WITH RankedScores AS (
    SELECT 
        Dev_ID,
        difficulty,
        score,
        RANK() OVER (PARTITION BY difficulty ORDER BY score ASC) AS ranking
    FROM Level_Details2
)
SELECT 
    Dev_ID,
    difficulty,
    score,
    ranking
FROM RankedScores
WHERE ranking <= 5
limit 5;

# 10. Find the device ID that is first logged in (based on `start_datetime`) for each player 
# (`P_ID`). Output should contain player ID, device ID, and first login datetime.
SELECT 
    P_ID,
    Dev_ID,
    MIN(timestamp) AS first_login_datetime
FROM 
    Level_Details2
GROUP BY 
    P_ID, Dev_ID;
    
# 11. For each player and date, determine how many `kill_counts` were played by the player 
# so far.
# a) Using window functions
# b) Without window functions

# a)with window funtions
SELECT
    P_ID,
    DATE(timestamp) AS date,
    SUM(kill_count) OVER (PARTITION BY P_ID ORDER BY timestamp) AS cumulative_kills
FROM
    Level_Details2
ORDER BY
    P_ID, timestamp ;

 # b) Without window functions

SELECT
    ld.P_ID,
    DATE(ld.timestamp) AS date,
    SUM(ld2.kill_count) AS cumulative_kills
FROM
    Level_Details2 ld
JOIN
    (SELECT P_ID, timestamp, kill_count FROM Level_Details2) ld2
ON
    ld.P_ID = ld2.P_ID
    AND ld.timestamp >= ld2.timestamp
GROUP BY
    ld.P_ID, DATE(ld.timestamp)
ORDER BY
    ld.P_ID, DATE(ld.timestamp);

# 12. Find the cumulative sum of stages crossed over `start_datetime` for each `P_ID`, 
# excluding the most recent `start_datetime

WITH CumulativeStages AS (
    SELECT 
        P_ID,
        Timestamp,
        stages_crossed,
        ROW_NUMBER() OVER (PARTITION BY P_ID ORDER BY  Timestamp DESC) AS rn
    FROM 
        Level_Details2
)
SELECT 
    P_ID,
     Timestamp,
    stages_crossed,
    SUM(stages_crossed) OVER (PARTITION BY P_ID ORDER BY  Timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS cumulative_stages
FROM 
    CumulativeStages
WHERE 
    rn > 1;

# 13. Extract the top 3 highest sums of scores for each `Dev_ID` and the corresponding `P_ID`.

WITH RankedScores AS (
    SELECT
        P_ID,
        Dev_ID,
        SUM(score) AS total_score,
        ROW_NUMBER() OVER (PARTITION BY Dev_ID ORDER BY SUM(score) DESC) AS ranking
    FROM
        Level_Details2
    GROUP BY
        P_ID, Dev_ID
)
SELECT
    P_ID,
    Dev_ID,
    total_score
FROM
    RankedScores
WHERE
    ranking <= 3
    limit 3 ;

# 14. Find players who scored more than 50% of the average score, scored by the sum of 
# scores for each `P_ID`.

WITH PlayerAvgScores AS (
    SELECT 
        P_ID,
        AVG(score) AS avg_score
    FROM 
        Level_Details2
    GROUP BY 
        P_ID
)
SELECT 
    ld.P_ID,
    ld.score,
    pas.avg_score
FROM 
    Level_Details2 ld
JOIN 
    PlayerAvgScores pas ON ld.P_ID = pas.P_ID
WHERE 
    ld.score > 0.5 * pas.avg_score;

# 15. Create a stored procedure to find the top `n` `headshots_count` based on each `Dev_ID` 
# and rank them in increasing order using `Row_Number`. Display the difficulty as well

CALL TopHeadshotsCountByDevId(5); -- Replace 5 with the desired value of n



