for ((  i = 1 ;  i <= 9;  i++  ))
do 
   curl http://localhost:6001/mupd8/slate/mupd8_demo/count_by_extension/$i
   echo ""
done
