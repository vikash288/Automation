PROJECT_DIR="/home/ubuntu/Automation/fullauto/DataEngineering/Scripts/"
JAR_LOC="/home/ubuntu/Automation/fullauto/DataEngineering/target/DataEngineering-jar-with-dependencies.jar"
PYTHON_FILE="CJAutomation.py"
LOAD_TYPE="full"

cd $PROJECT_DIR
python $PYTHON_FILE $JAR_LOC $LOAD_TYPE

