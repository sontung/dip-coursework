flume-ng agent -n flume_agent -f flume-conf

sqoop export --connect jdbc:postgresql://localhost/dip --username hduser -password hduser --table test --export-dir /user/hduser/out/browses-r-00000 --driver org.postgresql.Driver --connection-manager org.apache.sqoop.manager.GenericJdbcManager --direct --input-fields-terminated-by '\t' --lines-terminated-by '\n'

most viewed hours
SELECT * FROM time GROUP BY hr,count ORDER BY count DESC;

top tep browsed product
SELECT name,MAX(count) FROM br GROUP BY name,count ORDER BY count DESC LIMIT 10;
