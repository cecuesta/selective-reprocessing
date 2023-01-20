

while true
do
  FFF=data_`date +%Y%m%d_%H%M%S`
  export BASE=$RANDOM
  for NN in `seq $BASE $((BASE+9))`
  do
    echo $NN,$RANDOM
  done > $FFF
  mv $FFF flume-input/

  echo loaded file $FFF

  sleep 3
done 
