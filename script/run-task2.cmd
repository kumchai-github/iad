java -jar target/app.jar get data
java -jar target/app.jar get log
echo Start Task 2 of 2 | tee -a output/log/iedlog.txt
echo  ==== Job 1 of 4 -Import offering ==== | tee -a output/log/iedlog.txt
java -jar target/app.jar import offering | tee -a output/log/iedlog.txt
echo  ==== Job 2 of 4 -Export class ==== | tee -a output/log/iedlog.txt
java -jar target/app.jar export class encrypt | tee -a output/log/iedlog.txt
echo  ==== Job 3 of 4 -Export registration ==== | tee -a output/log/iedlog.txt
java -jar target/app.jar export registration encrypt | tee -a output/log/iedlog.txt
echo  ==== Job 4 of 4 -Export transcript ==== | tee -a output/log/iedlog.txt
java -jar target/app.jar export transcript encrypt | tee -a output/log/iedlog.txt
echo End Task 2 of 2 | tee -a output/log/iedlog.txt
echo ============ | tee -a output/log/iedlog.txt
java -jar target/app.jar put data
java -jar target/app.jar put log datestamp
java -jar target/app.jar archive blob input


