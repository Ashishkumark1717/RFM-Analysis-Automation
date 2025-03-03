CREATE OR ALTER VIEW RFM_MATRIX_PERC AS 
WITH CTE AS(

SELECT        S.FY, 
				C.CLIENT_ID, 
				C.CLIENT_NAME, 
				C.AGREEMENT_DATE, 
				S.FIRST_TRADE,
				S.LAST_TRADE,
				DATEDIFF(DAY, AGREEMENT_DATE, FIRST_TRADE) AS FIRST_TRADE_DIFF,
				S.INACTIVE_SINCE, 
				S.NO_OF_TRADES, 
                S.TRADED_DAYS,
				S.MKT_SESSIONS,
				S.GROSS_BRK, 
				S.NET_BRK, 
				S.TURNOVER,
				SUM(H.NOTIONAL_OR_CURRENT_VALUE_OF_HOLDING) AS HOLDINGS,
				M.[Total AUM],
				W.BRANCH_CODE, 
				W.BRANCH_NAME, 
				W.LEVEL_1, 
				W.LEVEL_2, 
				W.LEVEL_3, 
				W.LEVEL_4, 
				W.LEVEL_5, 
                W.CATEGORY_D_S AS DIRECT_SUB_BROKER, 
				CASE 
					WHEN PERCENT_RANK() OVER (ORDER BY INACTIVE_SINCE DESC) <= 0.33 THEN 1 
					WHEN PERCENT_RANK() OVER (ORDER BY INACTIVE_SINCE DESC) <= 0.66 THEN 2 
					ELSE 3
				END AS RECENCY, 
				CASE 
					WHEN PERCENT_RANK() OVER (ORDER BY NO_OF_TRADES) <= 0.33 THEN 1 
					WHEN PERCENT_RANK() OVER (ORDER BY NO_OF_TRADES) <= 0.66 THEN 2
				ELSE 3 
					END AS FREQUENCY, 
				CASE 
					WHEN CUME_DIST() OVER (ORDER BY GROSS_BRK) <= 0.33 THEN 1 
					WHEN CUME_DIST() OVER (ORDER BY GROSS_BRK) <= 0.66 THEN 2 
					ELSE 3 
				END AS VALUE
		FROM            
				SUMMARIZED_TRADE1_SP S 
		LEFT JOIN
                CLIENT_DETAIL1_SP C 
		ON 
				C.CLIENT_ID = S.CLIENT_ID
		LEFT JOIN
                WEALTH_EMPLOYEE_MASTER W 
		ON 
				C.BRANCH_CODE = W.BRANCH_CODE
		LEFT JOIN
				CLIENT_HOLDINGS_WEALTHONE_SP H
		ON 
				H.CLIENT_ID = S.CLIENT_ID
		LEFT JOIN
				TEMP_MF_CLIENTS M 
		ON 
				M.CLIENT_ID = S.CLIENT_ID
		WHERE        
				FY = 'FY 24-25'
		AND 
		C.CLIENT_ID IS NOT NULL
		GROUP BY 
				S.FY, 
				C.CLIENT_ID, 
				C.CLIENT_NAME, 
				C.AGREEMENT_DATE, 
				S.FIRST_TRADE,
				S.LAST_TRADE,
				S.INACTIVE_SINCE, 
				S.NO_OF_TRADES, 
                S.TRADED_DAYS,
				S.MKT_SESSIONS,
				S.GROSS_BRK, 
				S.NET_BRK, 
				S.TURNOVER,
				M.[Total AUM],
				W.BRANCH_CODE, 
				W.BRANCH_NAME, 
				W.LEVEL_1, 
				W.LEVEL_2, 
				W.LEVEL_3, 
				W.LEVEL_4, 
				W.LEVEL_5, 
                W.CATEGORY_D_S
UNION ALL
SELECT        S.FY, 
				C.CLIENT_ID, 
				C.CLIENT_NAME, 
				C.AGREEMENT_DATE, 
				FIRST_TRADE, 
				S.LAST_TRADE, 
				DATEDIFF(DAY, AGREEMENT_DATE, FIRST_TRADE) AS FIRST_TRADE_DIFF, 
				S.INACTIVE_SINCE, 
				S.NO_OF_TRADES, 
                S.TRADED_DAYS, 
				S.MKT_SESSIONS, 
				S.GROSS_BRK, 
				S.NET_BRK, 
				S.TURNOVER,
				SUM(H.NOTIONAL_OR_CURRENT_VALUE_OF_HOLDING) HOLDINGS,
				M.[Total AUM],
				W.BRANCH_CODE, 
				W.BRANCH_NAME, 
				W.LEVEL_1, 
				W.LEVEL_2, 
				W.LEVEL_3,
				W.LEVEL_4, 
				W.LEVEL_5, 
                W.CATEGORY_D_S AS DIRECT_SUB_BROKER,
				NULL AS Recency, 
				NULL AS Frequency,
				NULL AS Value
	FROM            
			SUMMARIZED_TRADE1_SP S 
	LEFT OUTER JOIN
            CLIENT_DETAIL1_SP C 
	ON 
			C.CLIENT_ID = S.CLIENT_ID 
	LEFT JOIN
            WEALTH_EMPLOYEE_MASTER W 
	ON 
			W.BRANCH_CODE = C.BRANCH_CODE
	LEFT JOIN
            CLIENT_HOLDINGS_WEALTHONE_SP H
	ON 
			H.CLIENT_ID = S.CLIENT_ID
	LEFT JOIN
            TEMP_MF_CLIENTS M 
	ON 
			M.CLIENT_ID = S.CLIENT_ID
	WHERE        FY = 'FY 21-24'
		AND 
		C.CLIENT_ID IS NOT NULL
	GROUP BY 
				S.FY, 
				C.CLIENT_ID, 
				C.CLIENT_NAME, 
				C.AGREEMENT_DATE, 
				S.FIRST_TRADE,
				S.LAST_TRADE,
				S.INACTIVE_SINCE, 
				S.NO_OF_TRADES, 
                S.TRADED_DAYS,
				S.MKT_SESSIONS,
				S.GROSS_BRK, 
				S.NET_BRK, 
				S.TURNOVER,
				M.[Total AUM],
				W.BRANCH_CODE, 
				W.BRANCH_NAME, 
				W.LEVEL_1, 
				W.LEVEL_2, 
				W.LEVEL_3, 
				W.LEVEL_4, 
				W.LEVEL_5, 
                W.CATEGORY_D_S
	),
	CTE2 AS
    (SELECT        *, 
					CONCAT_WS('', RECENCY, FREQUENCY, VALUE) AS MATRIX, 
					CASE 
						WHEN CUME_DIST() OVER (ORDER BY First_Trade_Diff DESC) <= 0.25 THEN 1 
						WHEN CUME_DIST() OVER (ORDER BY First_Trade_Diff DESC) <= 0.50 THEN 2 
						WHEN CUME_DIST() OVER (ORDER BY First_Trade_Diff DESC) <= 0.75 THEN 3 
						ELSE 4 
					END AS FIRST_TRADE_BUCKET, 
					CASE 
						WHEN INACTIVE_SINCE BETWEEN - 1 AND 90 THEN '0 to 3 Mon.' 
						WHEN INACTIVE_SINCE BETWEEN 91 AND 180 THEN '3 to 6 Mon.' 
						ELSE '>6 Mon' 
					END AS INACTIVE_BUCKET
		FROM            
				CTE
		WHERE        
				CLIENT_ID IS NOT NULL AND AGREEMENT_DATE >= '2024-04-01'
UNION ALL
		SELECT        *, 
						CONCAT_WS('', RECENCY, FREQUENCY, VALUE) AS MATRIX, 
						NULL AS FIRST_TRADE_BUCKET, 
						CASE 
							WHEN INACTIVE_SINCE BETWEEN - 1 AND 90 THEN '0 to 3 Mon.' 
							WHEN INACTIVE_SINCE BETWEEN 91 AND 180 THEN '3 to 6 Mon.' 
							ELSE '>6 Mon' 
						END AS INACTIVE_BUCKET
		FROM            
					CTE
		WHERE        
					CLIENT_ID IS NOT NULL AND AGREEMENT_DATE < '2024-04-01')
    SELECT        *, CASE 
							WHEN Matrix IN (333, 233) THEN 'Champions' 
							WHEN Matrix IN (322, 323) THEN 'Loyal Customers' 
							WHEN Matrix IN (223, 232, 313) THEN 'Potential Loyalists' 
							WHEN Matrix IN (221, 321, 312, 222, 212, 213, 332, 331) THEN 'Promising' 
							WHEN Matrix IN (311, 211, 231) THEN 'New customers' 
							WHEN Matrix IN (131, 122, 121) THEN 'At Risk' 
							WHEN Matrix IN (123, 133, 132, 113) THEN 'Cannot Lose Them' 
							WHEN Matrix IN (111, 112) THEN 'Hibernating' 
						END AS CATEGORY, 
						CASE 
							WHEN Matrix IN (333, 233) THEN 'Best Customers who are our ideal base' 
							WHEN Matrix IN (322, 323) THEN 'people who can be champions' 
							WHEN Matrix IN (223, 232, 313) THEN 'Need more convincing. Can become loyal' 
							WHEN Matrix IN (221, 321, 312, 222, 212, 213, 332, 331) THEN 'Showing interest - but could be unsure to transact large' 
							WHEN Matrix IN (311, 211, 231) THEN 'Who have shown interest' 
							WHEN Matrix IN (131, 122, 121) THEN 'Repeat Buyers Long Time Ago' 
							WHEN Matrix IN (123, 133, 132, 113) THEN 'Former Power Users, High Ticket One Timers.' 
							WHEN Matrix IN (111, 112) THEN 'Long Ago One Time Buyers' 
						END AS DESCRIPTION, 
						CASE 
							WHEN Matrix IN (333, 233) THEN 'Your Best Customer. who bought most recently, most often, and are heavy spenders.' 
							WHEN Matrix IN (322, 323) THEN 'Can become best, either stopping on revenue, or frequency. But they like you. Recent customers with average frequency and who spent a good amount.' 
							WHEN Matrix IN (223, 232, 313) THEN 'Recent customers with average frequency and who spent a good amount' 
							WHEN Matrix IN (221, 321, 312, 222, 212, 213, 332, 331) THEN 'Repeat Purchase or High Freq but avg to Lower ticket Size' 
							WHEN Matrix IN (311, 211, 231) THEN 'Recently Bought Low Ticket. Customers who have a high overall RFM score but are not frequent shoppers' 
							WHEN Matrix IN (131, 122, 121) THEN 'Who purchased often and spent big amounts, but haven’t purchased recently' 
							WHEN Matrix IN (123, 133, 132, 113) THEN 'Customers who used to visit and purchase quite often, but haven’t been visiting recently.' 
							WHEN Matrix IN (111, 112) THEN 'Who we have lost and were our former customer' 
						END AS CHARACTERISTICS, 
                        CASE 
							WHEN Matrix IN (333, 233) THEN 'Reward these customers. They can become early adopters for new products and will help promote your brand - Regular Meets and New Product ideas ' 
							WHEN Matrix IN (322, 323) THEN 'Offer membership or loyalty programs or recommend related products to upsell them. Cross Sell Uniquest' 
							WHEN Matrix IN (223, 232, 313) THEN 'Offer membership or loyalty programs or recommend related products to upsell them, Provide One to One Research Interaction' 
							WHEN Matrix IN (221, 321, 312, 222, 212, 213, 332, 331) THEN 'Engage with relevant Research & Advisory. Regular connect here is imp, Regular and systematic research recommendation through advisors. If not successful ideal base for Uniquest and MF Lumpsum' 
							WHEN Matrix IN (311, 211, 231) THEN 'Start building relationships with these customers by providing onboarding support and special offers to increase their Freq and Transaction Value Tailored product wise engagement program - 30D' 
							WHEN Matrix IN (131, 122, 121) THEN 'Personalized reactivation campaigns to reconnect, and offer renewals and helpful products to encourage another purchase Direct calling from RM and Dealer to Find out "Why" and plug the gap through Research. If not Equity then pitch Mutual Fund - LS'
							WHEN Matrix IN (123, 133, 132, 113) THEN 'Relevant promotions, and run surveys to find out what went wrong and avoid losing them to a competitor "Ideally pricing game and offers"' 
							WHEN Matrix IN (111, 112) THEN 'Re-activate them using Cross sell or offers "Prime candidates for reactivation and SIP' END AS ACTIONS
				FROM            
				
						CTE2