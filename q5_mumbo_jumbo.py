sqlQuery = """
select temp_3.genre, temp_4.u_id, temp_4.rating
from
(
select temp_1.genre as genre , MAx(temp_0.user_id) as user_id, MAX(temp_1.maxnum) as reviews
from
(select ratings._c0 as user_id, genres._c1 as genre, count(*) as number
 from  ratings inner join genres
 on ratings._c1 = genres._c0
 group by ratings._c0,genres._c1) temp_0
inner join
(select temp.genre as genre, MAX(temp.number) as maxnum
from
(select ratings._c0 as user_id, genres._c1 as genre, count(*) as number
from ratings inner join genres 
on ratings._c1 = genres._c0
group by ratings._c0, genres._c1) temp 
group by temp.genre
order by temp.genre asc) temp_1
on temp_0.genre = temp_1.genre 
and temp_0.number = temp_1.maxnum
group by temp_1.genre
order by temp_1.genre asc
) temp_3
inner join
(select ratings._c0 as u_id ,genres._c0 as genre, ratings._c2 as rating
 from  ratings inner join genres
 on  ratings._c1 = genres._c0
) temp_4
on 
    temp_4.u_id = temp_3.user_id

order by temp_3.genre asc

"""
