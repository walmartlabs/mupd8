
pidfile=mupd8.pid
messageserver_pidfile=messageserver.pid

# XXX: assume we run the code from ../target for now
target=`dirname $0 | pwd`/target
cpfile=$target/classpath.txt
mupd8jar=$target/mupd8-1.0-SNAPSHOT.jar
CLASSPATH=`head -1 $cpfile | tr -d '\n'`:$mupd8jar
exitCode=0

start() {
    export CLASSPATH=$CLASSPATH
    #export JAVA_OPT="-Xmx512M -Xms512M"

    echo "starting messageserver..."
    nohup java com.walmartlabs.mupd8.MessageServer -pidFile $messageserver_pidfile -d `pwd`/src/main/config/testapp >> nohup.messageserver.out 2>&1 &
    
     echo "starting the mupd8 app..."
#    nohup java com.walmartlabs.mupd8.Mupd8Main -pidFile $pidfile -key k1 -from file:$HOME/data/T10.data -to T10Source -threads 6 -s `pwd`/src/main/config/testapp/sys_old -a `pwd`/src/main/config/testapp/app_old &
#nohup java com.walmartlabs.mupd8.Mupd8Main -pidFile $pidfile -key k1 -from "file:`pwd`/src/test/resources/testapp/T10.data" -to T10Source -threads 6 -d `pwd`/src/main/config/testapp &
#nohup java com.walmartlabs.mupd8.Mupd8Main -pidFile $pidfile -key k1 -from "file:`pwd`/src/test/resources/testapp/T10.data" -to T10Source -threads 6 -d `pwd`/src/main/config/testapp >> nohup.mupd8.out 2>&1 &
#nohup java com.walmartlabs.mupd8.Mupd8Main -pidFile $pidfile -threads 6 -sc "com.walmartlabs.mupd8.JSONSource" -sp "file:`pwd`/src/test/resources/testapp/T10.data,K1" -to T10Source -d `pwd`/src/main/config/testapp >> nohup.mupd8.out 2>&1 &
     nohup java com.walmartlabs.mupd8.Mupd8Main -pidFile $pidfile -threads 6 -d `pwd`/src/main/config/testapp -statistics true>> nohup.mupd8.out 2>&1 &
}

stop() {
    if [ -f $pidfile ]; then
      mupd8_pid=`perl -nw -e '/^(\d+)$/ and print $1;' $pidfile`
      echo "stopping the mupd8 app ($mupd8_pid)..."
      kill $mupd8_pid
    else
      echo "Can't find mupd8 process"
      exitCode=1 
    fi
    if [ -f $messageserver_pidfile ]; then
      messageserver_pid=`perl -nw -e '/^(\d+)$/ and print $1;' $messageserver_pidfile`
      echo "stopping the messageserver ($messageserver_pid)..."
      kill $messageserver_pid
    else
      echo "Can't find messageserver process"
      exitCode=1
    fi
    exit $exitCode
}

status() {
   curl http://localhost:6001/app/status
}

case $1 in
    start)    start;;
    stop)     stop;;
    status)   status;;
    *)        echo "Usage: $0 start|stop|status" 1>&2; exit 1;;
esac
