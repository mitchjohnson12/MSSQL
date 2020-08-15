--Single Table
use clarity;
select	t.name , p.rows
from sys.tables t
	inner join sys.partitions p on t.object_id = p.object_id 
where t.name = 'clarity_tdl_tran'
	and p.index_id = 1

--All tables with RUT for master file
use clarity;
select	t.name , p.rows
from sys.tables t
	inner join sys.partitions p on t.object_id = p.object_id 
	inner join clarity_tbl tbl on tbl.table_name = t.name
	inner join clarity_tbl_2 tbl2 on tbl2.table_id = tbl.table_id
where p.index_id = 1
	and tbl2.track_row_update_yn = 'y'
	and tbl.table_id like 'c%'
order by p.rows desc
