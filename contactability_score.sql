BEGIN
	DROP TEMPORARY TABLE IF EXISTS summary;
	DROP TEMPORARY TABLE IF EXISTS tempp;	
	DROP TEMPORARY TABLE IF EXISTS tempp2;
	DROP TEMPORARY TABLE IF EXISTS ranked;
	DROP TEMPORARY TABLE IF EXISTS pivot;
	
	-- Calculate ptpSuccess
	
    CREATE TEMPORARY TABLE summary(
    SELECT a.lead_unique_identifier,
                                a.number, 
                                a.call_date,
                                a.final_disposition,
                                b.score as category,
                                CASE WHEN a.final_disposition = "PTP Arranged" THEN 1 ELSE 0 END AS ptp,
                                CASE WHEN a.final_disposition in ("HangUp","PTP Arranged","Refuses to Pay","No user responding","Request a Callback","CS Query Outstanding") THEN 1 ELSE 0 END AS rpc
    FROM collections_contactability.disposition AS a
    LEFT JOIN collections_contactability.dispos AS b
    ON a.final_disposition = b.disposition
    WHERE a.lead_unique_identifier is not null and lead_unique_identifier not in ('', ' '));

    -- SELECT * FROM summary WHERE lead_unique_identifier = '821343';

    CREATE TEMPORARY TABLE tempp(
    SELECT s.lead_unique_identifier,s.number,DATEDIFF(CURDATE(),MAX(s.call_date)) AS recency, IF(s.rpc > 0,((SUM(s.ptp)/SUM(s.rpc))*100),0) AS ptpSuccess,AVG(s.category) AS category,MAX(s.category) AS maxcat
    FROM summary as s 
    GROUP BY s.lead_unique_identifier,s.number);

    -- SELECT * FROM tempp WHERE lead_unique_identifier = '821343';

    -- Used to create range for recency encoding 
    SET @rmax = (SELECT ROUND(MAX(t.recency),1) FROM tempp as t);

    -- Adjust weights to final score as necessary
    SET @cWeight = 0.5;
    SET @sWeight = 0.35;
    SET @rWeight = 0.15;

    -- Encode recency: Oldest = 1 and earliest = 5
            
    CREATE TEMPORARY TABLE tempp2(index lead_unique_identifier(lead_unique_identifier), index number(number)) (
        SELECT t.lead_unique_identifier,
                                t.number,
                                CASE
                                        WHEN (t.recency <= @rmax/7)  AND t.maxcat = 5 THEN (ROUND((100 * @rWeight),2) + ROUND((t.category * 20 * @cWeight),2)  + ROUND((t.ptpSuccess * @sWeight),2) )
                                        WHEN (t.recency BETWEEN @rmax/7 AND @rmax/4) AND t.maxcat = 5 THEN (ROUND((80 * @rWeight),2) + ROUND((t.category * 20 * @cWeight),2)  + ROUND((t.ptpSuccess * @sWeight),2) )
                                        WHEN (t.recency BETWEEN @rmax/4 AND @rmax/2) AND t.maxcat = 5 THEN (ROUND((60 * @rWeight),2) + ROUND((t.category * 20 * @cWeight),2)  + ROUND((t.ptpSuccess * @sWeight),2) )
                                        WHEN (t.recency BETWEEN @rmax/2 AND @rmax/ 1) AND t.maxcat = 5 THEN (ROUND((40 * @rWeight),2) + ROUND((t.category * 20 * @cWeight),2)  + ROUND((t.ptpSuccess * @sWeight),2) )
                                        WHEN (t.recency >= @rmax) AND t.maxcat = 5 THEN (ROUND((20 * @rWeight),2) + ROUND((t.category * 20 * @cWeight),2)  + ROUND((t.ptpSuccess * @sWeight),2) )
                                        ELSE 0
                                END AS csrScore							
                                
        FROM tempp as t
        GROUP BY t.lead_unique_identifier,t.number);
            

    -- Import missing Contact detail from Rubix

    DROP TABLE IF EXISTS tempp3;	
    CREATE TEMPORARY TABLE tempp3 (index lead_unique_identifier(lead_unique_identifier), index number(number)) 
    -- SELECT * FROM tempp2;

    INSERT INTO tempp2 
    SELECT c2.contract_key, right(c.contact_detail*1, 9), 40 FROM rubix_clients.contact c
    LEFT JOIN rubix_blc.account a ON a.client_key = c.client_key
    LEFT JOIN rubix_blc.contract c2 ON c2.account_key = a.account_key
    JOIN dial_list dl ON dl.contract_key = c2.contract_key
    LEFT JOIN tempp3 t ON t.lead_unique_identifier = c2.contract_key AND t.number = right(c.contact_detail*1, 9)
    WHERE c.contact_type_key != 4
    AND left(c.contact_detail*1,1) > 5
    AND t.lead_unique_identifier IS NULL
    GROUP BY 1, 2;


    -- Import missing MSISDN from rubix

    DROP TABLE IF EXISTS tempp3;
    CREATE TEMPORARY TABLE tempp3 (INDEX lead_unique_identifier(lead_unique_identifier), INDEX number(number)) 

    INSERT INTO tempp2 
    SELECT c.contract_key, RIGHT(c.msisdn*1, 9), 70 FROM rubix_blc.deal_item_diary c
    JOIN dial_list dl ON dl.contract_key = c.contract_key
    LEFT JOIN tempp3 t ON t.lead_unique_identifier = c.contract_key AND t.number = right(c.msisdn*1, 9)
    WHERE left(c.msisdn*1,1) > 5
    AND t.lead_unique_identifier IS NULL
    GROUP BY 1, 2;		


    -- Import from contract_contactability

    DROP TABLE IF EXISTS import_new_contact;
    CREATE TEMPORARY TABLE import_new_contact
    SELECT contract_key, right(msisdn_rank1*1,9) number, 40 rank
    FROM contract_contactability c
    LEFT JOIN tempp2 t on c.contract_key = t.lead_unique_identifier and right(msisdn_rank1*1,9) = t.number
    WHERE t.lead_unique_identifier is null;

    insert into import_new_contact
    SELECT contract_key, right(msisdn_rank2*1,9) number, 40 rank
    FROM contract_contactability c
    LEFT JOIN tempp2 t on c.contract_key = t.lead_unique_identifier and right(msisdn_rank2*1,9) = t.number
    WHERE t.lead_unique_identifier is null;

    INSERT INTO import_new_contact
    SELECT contract_key, right(msisdn_rank3*1,9) number, 40 rank
    FROM contract_contactability c
    LEFT JOIN tempp2 t on c.contract_key = t.lead_unique_identifier and right(msisdn_rank3*1,9) = t.number
    WHERE t.lead_unique_identifier IS NULL;

    INSERT INTO tempp2
    SELECT * FROM import_new_contact
    GROUP BY 1, 2;
            
            
            -- SELECT * FROM tempp2 WHERE lead_unique_identifier = '821343';
            


            -- create ranks in contract key groups to rank numbers by csr score
    CREATE TEMPORARY TABLE ranked(
    SELECT lead_unique_identifier, number, csrScore, 
    @score_rank := IF(@current_rank = lead_unique_identifier, @score_rank + 1, 1) AS scoreRank ,
            @current_rank := lead_unique_identifier 
    FROM tempp2
    ORDER BY lead_unique_identifier,csrScore DESC);
        
        -- SELECT * FROM ranked WHERE lead_unique_identifier = '821343';
        
        -- pivot table 
    CREATE TEMPORARY TABLE pivot(
    SELECT
        lead_unique_identifier,
        SUM(CASE WHEN scoreRank = 1 THEN number ELSE 0 END) AS phone_0,
        SUM(CASE WHEN scoreRank = 2 THEN number ELSE 0 END) AS phone_1,
        SUM(CASE WHEN scoreRank = 3 THEN number ELSE 0 END) AS phone_2,
        SUM(CASE WHEN scoreRank = 1 THEN csrScore ELSE 0 END) AS phone_0_score,
        SUM(CASE WHEN scoreRank = 2 THEN csrScore ELSE 0 END) AS phone_1_score,
        SUM(CASE WHEN scoreRank = 3 THEN csrScore ELSE 0 END) AS phone_2_score
        FROM ranked
        GROUP BY lead_unique_identifier);
        
        -- SELECT * FROM pivot WHERE lead_unique_identifier = '821343';
        
        -- add primary key for faster update/join
        
    ALTER TABLE pivot ADD PRIMARY KEY (lead_unique_identifier(255));

        -- update dial list 
        
    UPDATE collections_contactability.dial_list as dl
    INNER JOIN pivot as p
    ON p.lead_unique_identifier = dl.contract_key
    SET dl.phone_0 = p.phone_0, 
                dl.phone_1 = p.phone_1, 
                dl.phone_2 = p.phone_2, 
                dl.phone_0_score = p.phone_0_score,
                dl.phone_1_score = p.phone_1_score,
                dl.phone_2_score = p.phone_2_score;

END