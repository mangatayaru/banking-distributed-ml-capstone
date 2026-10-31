USE banking;
SELECT COUNT(*) AS total_customers FROM bank_marketing;
SELECT y, COUNT(*) AS customers FROM bank_marketing GROUP BY y;
SELECT job, COUNT(*) AS customers,
       SUM(CASE WHEN y='yes' THEN 1 ELSE 0 END) AS subscriptions,
       ROUND(100.0*SUM(CASE WHEN y='yes' THEN 1 ELSE 0 END)/COUNT(*),2) AS conversion_pct
FROM bank_marketing GROUP BY job ORDER BY conversion_pct DESC;
SELECT month, COUNT(*) AS customers,
       SUM(CASE WHEN y='yes' THEN 1 ELSE 0 END) AS subscriptions,
       ROUND(100.0*SUM(CASE WHEN y='yes' THEN 1 ELSE 0 END)/COUNT(*),2) AS conversion_pct
FROM bank_marketing GROUP BY month ORDER BY conversion_pct DESC;
SELECT CASE WHEN pdays=-1 THEN 'not_previously_contacted' ELSE 'previously_contacted' END AS contact_history,
       y, COUNT(*) AS customers
FROM bank_marketing
GROUP BY CASE WHEN pdays=-1 THEN 'not_previously_contacted' ELSE 'previously_contacted' END, y;
