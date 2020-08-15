CREATE PROCEDURE Audit_CLARITY_ADT_QueryPlan
AS
	IF NOT EXISTS
	(
	    SELECT
			 1
	    FROM sys.objects
	    WHERE name = N'CLARITY_ADT_QueryPlan'
	)
		BEGIN
			CREATE TABLE CLARITY_ADT_QueryPlan
			(QueryText NVARCHAR(MAX),
			 QueryPlan XML,
			 Instant   DATETIME
			)
		END;
	INSERT INTO CLARITY_ADT_QueryPlan
		  SELECT
			    st.text
			  , qp.query_plan
			  , GETDATE()
		  FROM sys.dm_exec_requests r WITH (NOLOCK)
			  CROSS APPLY sys.dm_exec_sql_text(r.plan_handle) st CROSS APPLY sys.dm_exec_query_plan(r.plan_handle) qp
		  WHERE st.text LIKE '%CLARITY_ADT%'
			   AND st.text NOT LIKE '%adsfs%'
			   AND NOT EXISTS
		  (
			 SELECT
				   1
			 FROM CLARITY_ADT_QueryPlan caqp
			 WHERE caqp.QueryText = st.text
				  AND CAST(caqp.Instant AS DATE) = CAST(GETDATE() AS DATE)
		  )
GO
