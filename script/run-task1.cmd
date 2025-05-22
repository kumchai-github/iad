java -jar target/app.jar get data
java -jar target/app.jar delete blob input 10
java -jar target/app.jar delete blob output 10
java -jar target/app.jar delete blob log 10
java -jar target/app.jar archive blob output
java -jar target/app.jar archive blob log
echo Start Task 1 of 2 | tee output/log/iedlog.txt
echo  ==== Job 1 of 3 -Import registration ==== | tee -a output/log/iedlog.txt
java -jar target/app.jar import registration | tee -a output/log/iedlog.txt
echo  ==== Job 2 of 3 -Import offering ==== | tee -a output/log/iedlog.txt
java -jar target/app.jar import offering | tee -a output/log/iedlog.txt
echo  ==== Job 3 of 3 -Export course ==== | tee -a output/log/iedlog.txt
java -jar target/app.jar export course encrypt | tee -a output/log/iedlog.txt
echo End Task 1 of 2 | tee -a output/log/iedlog.txt
echo ============ | tee -a output/log/iedlog.txt
java -jar target/app.jar put data
java -jar target/app.jar put log
