-- ENSURE CORRECT SCHEMA IS SET

-- CREATE TWEETERS VIEW WITH INFLUENCE & STANCE SCORES
CREATE VIEW "Tweeters" AS
	SELECT t."user",
	 		CAST(
	 		   (CASE WHEN SP IS NULL THEN 0 ELSE SP * 5 END)
	 		 + (CASE WHEN WP IS NULL THEN 0 ELSE WP * 2 END)
	 		 - (CASE WHEN WN IS NULL THEN 0 ELSE WN * 2 END)
	 		 - (CASE WHEN SN IS NULL THEN 0 ELSE SN * 5 END)
	 		 AS INT) AS "stance",
	 		CAST(
	 		   ((CASE WHEN RRC IS NULL THEN 0 ELSE RRC END) + (CASE WHEN RTRC IS NULL THEN 0 ELSE RTRC END))
	 		   - ((CASE WHEN RSC IS NULL THEN 0 ELSE RSC END) + (CASE WHEN RTSC IS NULL THEN 0 ELSE RTSC END)) 
	 		 AS INT) AS "influence"
	 FROM (SELECT DISTINCT "user" FROM "Tweets") t
	 LEFT JOIN (
		SELECT "user", SUM(SP) AS SP, SUM(WP) AS WP, SUM(WN) AS WN, SUM(SN) AS SN
		 FROM "Tweets" t
		 LEFT JOIN (
			SELECT "id",
				SUM(CASE TA_TYPE WHEN 'StrongPositiveSentiment' THEN "total" END) AS SP,
				SUM(CASE TA_TYPE WHEN 'WeakPositiveSentiment' THEN "total" END) AS WP,
				SUM(CASE TA_TYPE WHEN 'WeakNegativeSentiment' THEN "total" END) AS WN,
				SUM(CASE TA_TYPE WHEN 'StrongNegativeSentiment' THEN "total" END) AS SN
			FROM (
				SELECT "id", TA_TYPE, COUNT(*) AS "total"
				 FROM "$TA_tweets"
				 WHERE  TA_TYPE = 'StrongPositiveSentiment' OR
						TA_TYPE = 'WeakPositiveSentiment' OR
						TA_TYPE = 'WeakNegativeSentiment' OR
						TA_TYPE = 'StrongNegativeSentiment'
				 GROUP BY "id", TA_TYPE
				)
			GROUP BY "id"
			) i ON t."id" = i."id"
		 GROUP BY "user"	  
	 	) s ON s."user" = t."user"
	 LEFT JOIN (
		SELECT "replyUser", COUNT(*) AS RRC 
		 FROM "Tweets"
		 WHERE "replyUser" != ''
		 GROUP BY "replyUser"
	 	) rrc ON rrc."replyUser" = t."user"
	 LEFT JOIN (
		SELECT "retweetedUser", COUNT(*) AS RTRC 
		 FROM "Tweets"
		 WHERE "retweetedUser" != '' 
		 GROUP BY "retweetedUser"
	 	) rtrc ON rtrc."retweetedUser" = t."user"
	 LEFT JOIN (
		SELECT "user", COUNT(*) AS RSC 
		 FROM "Tweets"
		 WHERE "replyUser" != '' 
		 GROUP BY "user"
	 	) rsc ON rsc."user" = t."user"
	 LEFT JOIN (
		SELECT "user", COUNT(*) AS RTSC 
		 FROM "Tweets"
		 WHERE "retweetedUser" != '' 
		 GROUP BY "user"
	 	) rtsc ON rtsc."user" = t."user"
	;
