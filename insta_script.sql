/* Questions for instagram_project 
4. Which user has the single most liked photo?
5. How many times does the average user post?
6. What are the top 5 most used hashtags?
7. We want to identify users who may be bots. Find the users who have liked every post?

Instagram project questions:
1.	Find total followers per user. 
2.	Find the name of the popular user by followers. - max followers 
3.	What is the engagement rate per follower ? 
               (ikes+comments/total followers * 100)
4.	Find the minimum number of hashtags used by any user.
5.	Find the photo with maximum comments.
		a.	Does the photo have maximum likes also? 
		b.	Find the photo with maximum likes
		c.	who does this photo belong to?
		d.	Is the user popular ?
		e.	Are the hashtags used popular ones? 
6.	Find the tags which attract maximum likes. 
(here we are calculating hashtags with high engagement)
7.	Find the percentage of active and inactive followers per user id.
8.	Find the popular time of active followers. (time is not different in this dataset ) 
9.	Do all the followers like all the photos of followee? - are they bots or not? 
10.	Find users using popular tags on their photos. 
(Are they having following among the top 10% of the users? )
Active followers : those who like/comment , inactive followers: who don’t do anything. 
take one popular user_id and analyse all the metrics with respect to him/her. */

-- We want to target our inactive users with an email campaign. 
-- 3. Find the users who have never posted a photo
use ig_clone;

-- WAY 1 
with photo_id as( select user_id, count(*) as users_with_photo from photos
                  group by user_id
				  )
select id , username, p.users_with_photo from users u
left join photo_id p
on u.id=p.user_id
where p.users_with_photo is null;

-- WAY 2 
select u.* from users u
left join photos p
on u.id=p.user_id
where p.user_id is null;

-- Find total followers per user. 
select u.id,u.username, count(f.followee_id) as total_followers 
from users u 
left join `follows` f
on u.id=f.followee_id
group by u.id;
-- if we have a already defined function in sql and our table is names after that then we use
-- Find the minimum, max number of hashtags used by any user on any photo. 
with final_table as (select u.username, p.id, t.tag_name as tag_names,
            				count(t.tag_name) as tag_count_per_photo
					from  users u 
					left join photos p on u.id=p.user_id 
					left join photo_tags pt on p.id=pt.photo_id 
					left join tags t on pt.tag_id=t.id
                    group by p.id  )
select distinct (u.username), 
			max(tag_count_per_photo) over(partition by username) as max_tags_used, 
			min(tag_count_per_photo) over (partition by username) as min_tags_used
from final_table 
right join users u on final_table.username=u.username;

-- What is the engagement rate per follower ?
-- even if we group the rows, we can still count the total rows by using over() --
-- count(*) over() 
with likes as (select f.follower_id , 
				count( l.photo_id) as total_likes 
                from follows f
                left join likes l on f.followee_id=l.user_id
                group by f.follower_id),
	comments as (select  f.follower_id,
				count( distinct (c.comment_text)) as total_comments
				from `follows` f
                join comments c on f.followee_id=c.user_id
				group by f.follower_id)
select tl.follower_id, tl.total_likes , 
		tc.total_comments,
        count(tl.follower_id) over() as total_followers,
        (tl.total_likes+ tc.total_comments) as engagement_per_user,
        ((tl.total_likes+ tc.total_comments)/count(tl.follower_id) over()) as engagement_rate
from likes tl
join comments tc
on tl.follower_id=tc.follower_id
group by tl.follower_id; 


-- find the tags attracting maximum likes 
select pt.photo_id, pt.tag_id, count(l.user_id) as likes_count 
from photo_tags pt 
left join likes l 
on pt.photo_id =l.photo_id
group by pt.tag_id;

-- 7.	Find the percentage of active and inactive followers per user id.
-- follower_id don't exist in photos table, likes table, comments table

with table_tocheck_inactive_followers as 
(
with follows_table as (
			with follower_nophoto as(
							select follower_id as photo_follower
							from `follows`
							where follower_id in ( (select distinct(user_id) from photos))
							group by photo_follower 
							),
				  follower_no_likes as(
							select follower_id as likes_follower
							from `follows`
							where follower_id in ( (select distinct(user_id) from likes))
							group by likes_follower
							),
				follower_no_comments as(
							select follower_id as comments_follower
							from `follows`
							where follower_id in ( (select distinct(user_id) from comments))
							group by comments_follower 
							)
			select f.follower_id, np.photo_follower ,nc.comments_follower,nl.likes_follower 
			from `follows` f 
			left join follower_nophoto as np on f.follower_id=np.photo_follower
			left join follower_no_likes as nl on f.follower_id=nl.likes_follower
			left join follower_no_comments as nc on f.follower_id=nc.comments_follower
        )
    select follower_id, 
    case 
    when (photo_follower is null) and (comments_follower is null) and (likes_follower is null) then 'inactive'
    else 'active' 
    end as follower_status 
    from follows_table
)
select follower_id from table_tocheck_inactive_followers
where follower_status ='inactive';

-- Find the photo with maximum comments. user,photo, comments,likes,hashtags,follower

create view view_photo_ques_table as(
	with photo_with_comments as(select p.id, count(c.id) as total_comments 
								from photos p
								join comments c 
								on p.id=c.photo_id
								group by p.id),
	photo_with_likes as  (select photo_id, count(user_id) as total_likes from likes
                            group by photo_id),
	photo_with_hashtags as (select p.id, group_concat(ph.tag_id) as tag_ids,
							group_concat(t.tag_name) as tag_names_used
							from photos p 
                            left join photo_tags ph on p.id=ph.photo_id
                            left join tags t on ph.tag_id=t.id
                            group by p.id),
	photo_with_followers as(select p.id, count(f.follower_id) as follower_count
							 from photos p 
                             left join `follows` f on p.user_id=f.followee_id 
                             group by p.id),
	photo_username as( select p.id as photoid,u.username
							from photos p 
                            left join users u on p.user_id=u.id 
							group by p.id)
		select pc.id , pu.username, 
        pc.total_comments,
        pl.total_likes, 
        ph.tag_ids,tag_names_used, 
        pf.follower_count
        from photo_with_comments pc 
        left join photo_with_likes pl on pc.id=pl.photo_id
        left join photo_username pu on pc.id=pu.photoid
        left join photo_with_hashtags ph on pc.id=ph.id
        left join photo_with_followers pf on pc.id=pf.id

);
-- select photo with maximum comments 
select * from view_photo_ques_table
order by total_comments desc;

-- select photo with max likes
select * from view_photo_ques_table
order by total_likes desc;

-- c.	who does this photo belong to? --> harley_lind
select * from view_photo_ques_table
order by total_comments desc;

-- d.	Is the user popular ? - has 76 followers so not that popular 

-- e. Are the hashtags used popular ones? 
with popular_tag as (select tags_table.tag_id from 
						(	select tag_id, count(photo_id) as tag_count
							from photo_tags
							group by tag_id
							order by tag_count desc limit 1
							 )tags_table
                        )
select id as photo_posted ,total_comments, total_likes,tag_ids,tag_names_used
from view_photo_ques_table 
where (total_comments=(select max(total_comments) from view_photo_ques_table)) 
		OR 
	(total_likes =(select max(total_likes) from view_photo_ques_table)) 
		AND
(tag_ids like concat('%',(select popular_tag.tag_id from popular_tag),'%'));


-- Does the photo have maximum likes also?  - 
-- not work because we don't have primary key in likes and comments table
select p.id, count(c.comment_text) as total_comments, count(l.created_at) as total_likes 
from photos p 
left join comments c on c.photo_id = p.id
right outer join likes l on l.photo_id=p.id
group by p.id
order by total_comments desc;

-- why is joins not giving the result - 
-- because we are combining multiple table having duplicate values

-- Through subqueries
/*Find the photo with maximum comments.
		a.	Does the photo have maximum likes also? 
		b.	Find the photo with maximum likes
		c.	who does this photo belong to?
		d.	Is the user popular ?
		e.	Are the hashtags used popular ones? */

			select likes_comment_table.photoid,
					photo_posters_table.total_photo_posters,
					likes_comment_table.total_comments,
					likes_comment_table.total_likes
			from
					(select p.id as photoid, u.username as total_photo_posters from photos p 
					left join users u on u.id=p.user_id group by p.id ) photo_posters_table       
			join
						(select * from (
									(select p.id as photoid, count(c.comment_text) as total_comments
										from photos p 
										left join comments c on c.photo_id = p.id
										group by p.id) com
								inner join 
									(select p.id as photo__id , count(l.created_at) as total_likes
										from photos p 
										left join likes l on p.id = l.photo_id
										group by p.id) lik
								on com.photoid = lik.photo__id
									   )
							)likes_comment_table
			on photo_posters_table.photoid=likes_comment_table.photoid;

-- column names can be different in joining. 

------------------------------------------------------------------------------------------
-- Find the percentage of active and inactive followers per user id.

select f.follower_id, p.id as photo_id, l.photo_id as likes
from `follows` f 
left join photos p on f.followee_id= p.user_id
left join likes l on f.followee_id =p.user_id
group by f.follower_id;

-- (+total_comments)/count(f.follower_id) as engagement_rate


select f.follower_id,
count(distinct l.photo_id) as total_likes
from `follows` f 
left join likes l on f.follower_id=l.user_id
group by f.follower_id;

select f.follower_id,
count(distinct c.photo_id) as total_comments
from `follows` f 
left join comments c on f.follower_id=c.user_id
group by f.follower_id;
-- What is the engagement rate per follower ? (ikes+comments/total followers * 100)
-- bots => people who have liked and commented on all the photos posted till date
select l.*,c.total_comments,
count(distinct p.id) as photo_posted, 
count(l.follower_id) over() as total_followers, 
round((total_likes+total_comments)/(count(l.follower_id) over()),2)*100 as engagement_pct
from likes_table l
join comments_table c on l.follower_id=c.follower_id
left join photos p on l.follower_id= p.user_id
group by l.follower_id;

select f.follower_id, count(distinct p.id) as photo_posted
from `follows` f 
left join photos p on f.follower_id= p.user_id
group by f.follower_id;

/*
7.	Find the percentage of active and inactive followers per user id.
8.	Find the popular time of active followers. (time is not different in this dataset ) 
9.	Do all the followers like all the photos of followee? - are they bots or not? 
10.	Find top 10 followers who are active, are they following people who are using popular tags.   . 
(Are they having following among the top 10% of the users? )
*/
-- likes and comments per photo id, user posting photo_id, active inactive, bots, not a follower, 

select u.id, u.username, 
f.total_likes,f.total_comments,
count(distinct p.id) as total_photos_posted,
f.engagement_pct, 
case 
      when engagement_pct  IS NULL then 'inactive follower'
      when engagement_pct  IS NULL and count(distinct p.id)=0 then 'not a follower'
	  else 'active follower'
end 'status'
from users u
left join photos p on u.id=p.user_id
left join follower_engagement_view f on u.id=f.follower_id  
group by u.id;

create view followers_activity as
(
select u.id, u.username, 
f.total_likes,f.total_comments,
count(distinct p.id) as total_photos_posted,
f.engagement_pct 
from users u
left join photos p on u.id=p.user_id
left join follower_engagement_view f on u.id=f.follower_id  
group by u.id);

select f.*, 
case 
when engagement_pct is null and total_photos_posted >0 then 'not a follower'
when engagement_pct is null and total_photos_posted=0 then 'inactive follower' 
when engagement_pct>0  and total_photos_posted>0  then ' active follower'
when total_likes = 257 and total_comments=257 then 'bot' 
end 'status' 
from followers_activity f;

-- photo posted by follower
select f.follower_id, count(distinct p.id) as photo_posted
from `follows` f 
left join photos p on f.follower_id= p.user_id
group by f.follower_id;

-- one follower-> one photo_id -> find the number of comments done per photo id by one follower 
-- find out all the users who have commented more than once on a photo_id  
with user_ids as (
				with comments_per_user as (
				select user_id, photo_id, comment_text 
				from comments)
				select u.*, count(*) 
				from comments_per_user u
				group by user_id,photo_id
				having count(*)>1 
)
select * 
from user_ids u 
left join follows f 
on f.follower_id = u.user_id;
-- meaning none of the followers commented twice on the same photo

with new_table as (
with active_follower_top_10 as (
								select id, username, status, engagement_pct from follower_identifier 
								where status =' active follower'
								order by engagement_pct desc 
								limit 10
                                )
select af.*, f.followee_id as users_followed 
from active_follower_top_10 af
right join follows f on af.id=f.follower_id
) 
select * from new_table n
right join users_tags_usage ut on n.users_followed = ut.user_id; 


-- users who posted photos used these tags

with users_tags_usage as (
					select u.id as user_id , group_concat(distinct p.id) as photos_posted,
					group_concat(distinct pt.tag_id) as tags_used_in_photos
					from users u
					left join photos p on u.id=p.user_id
					left join photo_tags pt on p.id=pt.photo_id 
					group by u.id
                    )
select * from users_tags_usage

-- enagement percentage per hash tag id. 
-- hashtags sorted by likes and comments 
-- hashtags sorted by likes and comments , used by which top 5 users? 



with likes_pct as (
					select pt.tag_id, count(l.user_id) as likes_per_tag
					from photo_tags pt 
					join likes l on pt.photo_id =l.photo_id 
					group by pt.tag_id
					order by count(l.user_id) desc
                    )
select *, sum(likes_per_tag) over ()as total_likes,
likes_per_tag /(sum(likes_per_tag) over())*100 as tag_likes_engagement_pct
from likes_pct 
group by tag_id;

with tag_com_pct as(
			with comments_pct as (
								select pt.tag_id, count(c.user_id) as comments_per_tag
								from photo_tags pt 
								join comments c on pt.photo_id =c.photo_id 
								group by pt.tag_id
								order by count(c.user_id) desc
								)
			select *, sum(comments_per_tag) over ()as total_comments,
			comments_per_tag /(sum(comments_per_tag) over())*100 as tag_comments_engagement_pct
			from comments_pct 
			group by tag_id
)
select tc.*,tp.tag_likes_engagement_pct, tp.likes_per_tag, tp.total_likes
from tag_com_pct tc
join tag_likes_performance tp on
tc.tag_id=tp.tag_id;

-- top 10 tags 
select tag_id,tag_comments_engagement_pct,tag_likes_engagement_pct 
from tag_performance
order by likes_per_tag desc 
limit 5;

create view tag_view as(
with cte as(
SELECT tag_id FROM tag_performance
order by tag_comments_engagement_pct desc
limit 5)
select group_concat(tag_id) as popular_tags_used from cte);

SELECT * FROM followers_photo_tag 
where tags_used_in_photos 
like concat("%",(select popular_tags_used from tag_view),"%")
