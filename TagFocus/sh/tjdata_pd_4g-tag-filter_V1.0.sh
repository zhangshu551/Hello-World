#!/bin/sh
baseDirForScriptSelf=$(cd "$(dirname "$0")"; pwd)
date=$1
configDir=$2
configPath=${configDir}tjdata_tag/
imeiProvStr=""
currentMonth=`date -d "-2 month" +%Y%m`
province=('811' '812' '813' '814' '815' '821' '822' '823' '831' '832' '833' '834' '835' '836' '837' '841' '842' '843' '844' '845' '846' '850' '851' '852' '853' '854' '861' '862' '863' '864' '865')
for i in ${province[*]}
do
        imeiProvStr=$imeiProvStr$i","
done
imeiProvStr=${imeiProvStr%,}
if [ `date +%d` -gt "05" ] ; then
        currentMonth=`date -d "-1 month" +%Y%m`
fi
run () {
        spark-submit --master yarn \
        --class com.tjdata.spark.main.TagFocusMain \
        --deploy-mode cluster \
        --executor-memory 10g \
        --num-executors 30 \
        --executor-cores 20 \
        --driver-memory 10g \
        --driver-cores 3 \
        --conf "spark.default.parallelism"=500 \
        --conf "spark.sql.shuffle.partitions"=500 \
        --conf "spark.shuffle.memoryFraction"=0.6 \
        --conf "spark.storage.memoryFraction"=0.3 \
        --conf "spark.network.timeout"=720 \
        $baseDirForScriptSelf/tag_focus.jar $date $configPath $imeiProvStr $currentMonth
}
hadoop fs -test -e $configDir
if [ $? -eq 0 ] ;then
      hadoop fs -rm -r $configDir
      echo 'Config Directory is deleted'
fi
hadoop fs -put $baseDirForScriptSelf/tjdata_tag $configDir

if [ -n "$date" -a -n "$configDir" ] ; then
        run $date $configPath $imeiProvStr $currentMonth > /data11/dacp/vendorszjsb/logs/tag/`date +%Y%m%d`.log 2>&1 &
else
        echo "both parameters are empty!"
fi