PROJECT_DIR="/home/ubuntu/Automation/fullauto/DataEngineering/Scripts/"
JAR_LOC="/home/ubuntu/Automation/fullauto/DataEngineering/target/DataEngineering-jar-with-dependencies.jar"
PYTHON_FILE="LinkShareAutomation.py"
LOAD_TYPE="incremental"

cd $PROJECT_DIR
python $PYTHON_FILE $JAR_LOC $LOAD_TYPE
