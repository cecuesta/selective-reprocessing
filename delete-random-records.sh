


for ID in $(  curl localhost:19200/index-data/_search?pretty 2>/dev/null | grep _id | awk '{ print $3}' | sed -e 's/"//g' -e 's/,//' )
do
  echo deleting record : $ID
  curl -XDELETE localhost:19200/index-data/_doc/$ID
  echo

done


