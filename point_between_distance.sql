create table point_between_distance
    (point_name string comment 'Name of point'
    ,next_point_name string comment 'Name of next point'
    ,distance int comment 'Distance between the points'
    ,process_dttm timestamp comment 'Date and time of calculation'    
    )
    partitioned by (day string comment 'Date of calculation')
    stored as orc
    tblproperties ("org.compress"="ZLIB");