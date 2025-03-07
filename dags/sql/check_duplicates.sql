SELECT callsign, timestamp, count(*) as cnt
FROM {target_table}
GROUP BY 1,2
HAVING cnt > 1