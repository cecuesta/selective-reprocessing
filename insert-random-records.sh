
for NN in {1..10}
do
  echo inserting record $NN ------------------------
  curl -X POST "localhost:19200/index-data/_doc/" -H 'Content-Type: application/json' -d "{ \"filename\" : \"$FILENAME\",  \"line_number\" : $NN }"
  echo
  echo
done

