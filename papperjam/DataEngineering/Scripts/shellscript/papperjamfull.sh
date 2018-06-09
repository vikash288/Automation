PROJECT_DIR="/home/ubuntu/Automation/papperjam/DataEngineering/Scripts/"
JAR_LOC="/home/ubuntu/Automation/papperjam/DataEngineering/target/DataEngineering-jar-with-dependencies.jar"
PYTHON_FILE="PapperjamAutomation.py"
LOAD_TYPE="full"

cd $PROJECT_DIR
python $PYTHON_FILE $JAR_LOC $LOAD_TYPE
