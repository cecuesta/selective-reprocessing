
FILE_TMP=/tmp/data.$$.tmp

ESHOST=localhost:19200

docker exec -it mysql mysql -u myuser -prootpass -e "select status, filename, records from info_files where status = 'PENDING' and ts_insert <  date_sub(sysdate(), interval 5 minute) ;" mydatabase | grep PENDING | sed -e "s/ //g"   > $FILE_TMP

for LINE in $(cat $FILE_TMP)
do
  DATAFILE=$(echo $LINE | awk -F "|" '{ print $3}' )
  NUM_RECS=$(echo $LINE | awk -F "|" '{ print $4}' )
  echo $DATAFILE "-" $NUM_REGS

  NUM_RECS_IN_ES=$(curl -s "$ESHOST/index-data/_search?pretty&size=0&track_total_hits=true&q=filename:$DATAFILE" | grep total -A1 | tail -1 | sed -e "s/,//g" | awk '{ print $NF ;}')

  echo -${NUM_RECS_IN_ES}-
  echo -${NUM_RECS_IN_ES}-

  if [ "$NUM_RECS_IN_ES" == "" ] ; then NUM_RECS_IN_ES=0 ; fi

  STATUS=OK
  if [ "$NUM_RECS" -ne "$NUM_RECS_IN_ES" ]; then STATUS=KO ; fi

  QUERY_UPD="update info_files set status = '$STATUS',  records_es = $NUM_RECS_IN_ES where filename = '$DATAFILE'"

  echo $QUERY_UPD

  docker exec -it mysql mysql -u myuser -prootpass -e "$QUERY_UPD" mydatabase

done

docker exec -it mysql mysql -u myuser -prootpass -e "select * from info_files" mydatabase

rm $FILE_TMP

