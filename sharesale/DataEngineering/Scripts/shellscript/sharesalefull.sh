PROJECT_DIR="/home/ubuntu/Automation/sharesale/DataEngineering/Scripts/"
JAR_LOC="/home/ubuntu/Automation/sharesale/DataEngineering/target/DataEngineering-jar-with-dependencies.jar"
PYTHON_FILE="SharesaleAutomation.py"
LOAD_TYPE="incremental"

cd $PROJECT_DIR
python $PYTHON_FILE $JAR_LOC $LOAD_TYPE
